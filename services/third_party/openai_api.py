import asyncio
import json
import random
from typing import Optional

import openai
from openai import AsyncOpenAI
from pydantic import ValidationError

from core.config import get_settings
from core.logger import logger
from models.openai_models import Message

config = get_settings()
client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)


def __exponential_backoff_with_jitter(
        attempt: int,
        base_delay: float = config.API_BASE_DELAY,
        max_delay: float = config.API_MAX_DELAY,
) -> float:
    """Calculate delay with exponential backoff and jitter to avoid thundering herd"""
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0, 0.1 * delay)  # Add 0-10% jitter
    return delay + jitter


def __is_retryable_error(error) -> bool:
    """Determine if an error should trigger a retry"""
    if isinstance(error, openai.InternalServerError):
        return True
    elif isinstance(error, openai.RateLimitError):
        return True
    elif isinstance(error, openai.APITimeoutError):
        return True
    elif isinstance(error, openai.APIConnectionError):
        return True
    elif hasattr(error, "status_code") and error.status_code in [429, 500, 502, 503, 504]:
        return True
    return False


def async_retry_on_api_error(max_retries: int = config.API_MAX_RETRIES):
    """Async decorator to add retry logic to any function that calls OpenAI API"""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as error:
                    last_error = error

                    if not __is_retryable_error(error):
                        raise error  # Re-raise non-retryable errors

                    if attempt == max_retries:
                        break

                    delay = __exponential_backoff_with_jitter(attempt)
                    logger.info(f"Attempt {attempt + 1} failed: {error}")
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)

            raise last_error  # Raise the last error if all attempts failed

        return wrapper

    return decorator


@async_retry_on_api_error(max_retries=config.API_MAX_RETRIES)
async def parse_discord_message_using_openai(
        message: str,
        model_id: str = config.OPENAI_MODEL_ID,
        attempt: int = 1
) -> Optional[Message]:
    """
    Parse Discord message using OpenAI API with async support

    Args:
        message (str): The Discord message to parse
        model_id (str): OpenAI model ID to use
        attempt (int): Current attempt number for internal retry logic

    Returns:
        Optional[Message]: Parsed message object or None if parsing fails
    """
    response = await client.chat.completions.create(
        model=model_id,
        messages=[
            {
                "role": "system",
                "content": "You are a trading assistant that extracts structured information from crypto trading signals.",
            },
            {
                "role": "user",
                "content": message,
            }
        ],
        tools=[openai.pydantic_function_tool(Message)],
        timeout=30,
    )

    try:
        parsed_message = Message(**json.loads(
            response.choices[0].message.tool_calls[0].function.arguments
        ))
        return parsed_message

    except (TypeError, KeyError, json.JSONDecodeError, IndexError, ValidationError) as e:
        logger.error(f"Failed to parse: {response}")
        logger.error(f"{type(e).__name__}: {e}")

        if attempt > config.API_MAX_RETRIES:
            return None

        delay = __exponential_backoff_with_jitter(attempt)
        await asyncio.sleep(delay)
        return await parse_discord_message_using_openai(
            message=message,
            model_id=model_id,
            attempt=attempt + 1
        )
