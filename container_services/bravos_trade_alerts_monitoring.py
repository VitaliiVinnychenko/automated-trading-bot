import asyncio
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup

from core.config import get_settings, Settings
from core.logger import logger
from services.db_service import DbService
from services.third_party.telegram_api import TelegramNotifier


class TradeAlertsScraper:
    def __init__(self, config: Settings, db_service: DbService, telegram_notifier: TelegramNotifier):
        """
        Initialize the scraper with cookie and Redis connection.

        Args:
            config: Settings configuration object
            db_service: Database service instance
            telegram_notifier: Telegram notification service
        """
        self.url = "https://bravosresearch.com/category/portfolio-update/"
        self.headers = {
            'Cookie': config.BRAVOS_COOKIE,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.db_service = db_service
        self.telegram_notifier = telegram_notifier

    async def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch the HTML content of a page.

        Args:
            url: URL to fetch

        Returns:
            HTML content or None if failed
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        url,
                        headers=self.headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    response.raise_for_status()
                    html = await response.text()
                    logger.debug(f"Successfully fetched page: {url}")
                    return html
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching page {url}: {e}")
            return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching page: {url}")
            return None

    def extract_article_links(self, html: str) -> List[str]:
        """
        Extract article links from the HTML content.

        Args:
            html: HTML content as string

        Returns:
            List of article URLs in order of appearance
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Find the posts grid container
        posts_grid = soup.find('div', class_='co_posts_grid')
        if not posts_grid:
            logger.warning("Could not find co_posts_grid div")
            return []

        # Find all post_single divs
        articles = posts_grid.find_all('div', class_='post_single')
        logger.info(f"Found {len(articles)} articles")

        links = []
        for article in articles:
            # Find the "Read More" button
            button = article.find('a', class_='button')
            if button and button.get('href'):
                link = button['href']
                links.append(link)
                logger.debug(f"Found article link: {link}")

        return links

    def extract_article_content(self, html: str, url: str) -> Optional[dict]:
        """
        Extract article title and content from article page.

        Args:
            html: HTML content of article page
            url: Article URL

        Returns:
            Dictionary with title and content, or None if extraction failed
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Extract title from entry-header h1
            title_tag = soup.find('header', class_='entry-header')
            if title_tag:
                title_h1 = title_tag.find('h1', class_='entry-title')
                title = title_h1.get_text(strip=True) if title_h1 else "No Title"
            else:
                title = "No Title"

            # Extract article content from entry-content div
            content_area = soup.find('div', class_='entry-content')

            if not content_area:
                logger.warning(f"Could not find entry-content div for {url}")
                return None

            # Find the container div inside entry-content
            container = content_area.find('div', class_='container')
            if not container:
                logger.warning(f"Could not find container div for {url}")
                return None

            # Extract all <p> tags, ignoring images and other elements
            paragraphs = container.find_all('p')
            content_parts = []

            for p in paragraphs:
                # Get text content, stripping whitespace
                text = p.get_text(strip=True)

                # Skip empty paragraphs and paragraphs that only contain &nbsp;
                if text and text != '\xa0':
                    content_parts.append(text)

            if not content_parts:
                logger.warning(f"No paragraph content extracted from {url}")
                return None

            # Join paragraphs with double newlines for readability
            content = '\n\n'.join(content_parts)

            logger.info(f"Successfully extracted article: {title}")
            logger.debug(f"Extracted {len(content_parts)} paragraphs, {len(content)} characters")

            return {
                'title': title,
                'content': content,
                'url': url
            }

        except Exception as e:
            logger.error(f"Error extracting article content from {url}: {e}")
            return None

    async def fetch_and_process_article(self, article_url: str) -> bool:
        """
        Fetch an article and send its content to Telegram.

        Args:
            article_url: URL of the article to process

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Processing article: {article_url}")

        # Fetch article page
        html = await self.fetch_page(article_url)
        if not html:
            return False

        # Extract article content
        article_data = self.extract_article_content(html, article_url)
        if not article_data:
            return False

        # Format message for Telegram
        message = self.format_telegram_message(article_data)

        # Send to Telegram
        try:
            await self.telegram_notifier.send_article_update(message)
            logger.info(f"Successfully sent article to Telegram: {article_data['title']}")
            return True
        except Exception as e:
            logger.error(f"Failed to send article to Telegram: {e}")
            return False

    def format_telegram_message(self, article_data: dict) -> str:
        """
        Format article data into a Telegram message.

        Args:
            article_data: Dictionary with title, content, and url

        Returns:
            Formatted message string
        """
        # Telegram message length limit is 4096 characters
        max_content_length = 3500  # Leave room for title and URL

        title = article_data['title']
        content = article_data['content']
        url = article_data['url']

        # Truncate content if too long
        if len(content) > max_content_length:
            content = content[:max_content_length] + "..."

        message = f"<b>{title}</b>\n\n"
        message += f"{content}\n\n"
        message += f"<a href=\"{url}\">Read full article</a>"

        return message

    async def get_new_articles(self, article_links: List[str]) -> List[str]:
        """
        Get list of new articles that haven't been processed yet.

        Args:
            article_links: List of all article URLs from the page

        Returns:
            List of new article URLs to process
        """
        if not article_links:
            return []

        latest_url = await self.db_service.get_latest_bravos_article_url()

        # If no previous URL exists, return only the first article
        if not latest_url:
            logger.debug("No previous article found, returning first article only")
            return [article_links[0]]

        # If the first URL is the same as stored, no new articles
        if article_links[0] == latest_url:
            logger.debug("No new articles found")
            return []

        # Find where the latest URL is in the list
        new_articles = []
        for url in article_links:
            if url == latest_url:
                break
            new_articles.append(url)

        logger.info(f"Found {len(new_articles)} new articles")
        return new_articles

    async def scrape(self) -> List[str]:
        """
        Main scraping method that orchestrates the entire process.

        Returns:
            List of new article URLs that were processed
        """
        logger.debug("Starting scrape operation")

        # Fetch the main page
        html = await self.fetch_page(self.url)
        if not html:
            return []

        # Extract article links
        article_links = self.extract_article_links(html)
        if not article_links:
            logger.warning("No article links found")
            return []

        # Get new articles
        new_articles = await self.get_new_articles(article_links)
        new_articles = new_articles[::-1]

        if not new_articles:
            logger.debug("No new articles to process")
            return []

        # Process each new article
        processed_articles = []
        for article_url in new_articles:
            success = await self.fetch_and_process_article(article_url)
            if success:
                processed_articles.append(article_url)
            # Small delay between processing articles
            await asyncio.sleep(2)

        # Update Redis with the latest article only if we successfully processed at least one
        if processed_articles:
            await self.db_service.set_latest_bravos_article_url(new_articles[-1])
            logger.info(f"Successfully processed {len(processed_articles)} articles")

        return processed_articles

    async def run_continuous(self, interval_seconds: int = 300):
        """
        Run the scraper continuously at specified intervals.

        Args:
            interval_seconds: Time between scrapes (default: 300 seconds = 5 minutes)
        """
        logger.info(f"Starting continuous scraping with {interval_seconds}s interval")

        while True:
            try:
                processed_articles = await self.scrape()

                if processed_articles:
                    logger.info(f"Processed {len(processed_articles)} new articles")
                else:
                    logger.debug("No new articles to process")

                logger.info(f"Sleeping for {interval_seconds} seconds")
                await asyncio.sleep(interval_seconds)

            except KeyboardInterrupt:
                logger.info("Scraper stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                await asyncio.sleep(interval_seconds)


async def main():
    """Example usage of the scraper."""
    config = get_settings()
    db = DbService(config)
    telegram = TelegramNotifier(logger)

    # Initialize scraper
    scraper = TradeAlertsScraper(
        config=config,
        db_service=db,
        telegram_notifier=telegram
    )

    try:
        await scraper.run_continuous(interval_seconds=300)
    finally:
        await telegram.close_session()


if __name__ == "__main__":
    asyncio.run(main())
