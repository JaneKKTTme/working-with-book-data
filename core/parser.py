import asyncio
import signal
import sys
import time
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from config import SERVICE, BOOK_PAGE, REQUEST_CONFIG
from core.http_client import HTTPClient
from models.book import Book
from parsers.list_parser import ListParser
from parsers.book_parser import BookParser
from storage.parquet_storage import ParquetStorage
from logging_config import logger


class BookvoedParser:

    def __init__(self, max_concurrent_tasks: int = REQUEST_CONFIG['concurrent_tasks'],
                 delay: float = REQUEST_CONFIG['delay_between_requests']):
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.shutdown_event = asyncio.Event()
        
        self.http_client = HTTPClient(delay=delay)
        self.storage = ParquetStorage()

        self.executor = ThreadPoolExecutor(max_workers=4)
        
        if sys.platform != 'win32':
            self._setup_signal_handlers()

    async def __aenter__(self):
        await self.http_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def parse_bookvoed(self, start_page: int = 1) -> None:
        page = start_page
        has_next_page = True
        total_books = 0
        start_time = time.time()

        with tqdm(desc='Total pages processed', unit='page') as pbar:
            while has_next_page and not self.shutdown_event.is_set():
                books_on_page, has_next_page = await self._parse_book_list(page)

                if books_on_page:
                    added_books = self.storage.save_books(books_on_page)
                    total_books += added_books

                    elapsed = time.time() - start_time
                    books_per_second = total_books / elapsed if elapsed > 0 else 0
                    pbar.set_postfix({
                        'books': total_books,
                        'speed': f'{books_per_second:.1f} books/s'
                    })

                    logger.info(f'Page {page} processed. Found: {len(books_on_page)}, '
                              f'Added: {added_books}, Next: {has_next_page}')

                pbar.update(1)
                page += 1
                await asyncio.sleep(0.5)

    async def close(self):
        self.executor.shutdown(wait=True)
        await self.http_client.close()

    async def shutdown(self):
        logger.info('Shutting down gracefully...')
        self.shutdown_event.set()

    def _setup_signal_handlers(self):
        for s in (signal.SIGTERM, signal.SIGINT):
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(s, lambda: asyncio.create_task(self.shutdown()))

    async def _parse_book_list(self, page: int) -> tuple[List[Book], bool]:
        curr_url = SERVICE + BOOK_PAGE + str(page)
        
        try:
            html = await self.http_client.get(curr_url)
            if not html:
                return [], False

            list_parser = ListParser(html, page)
            
            if not list_parser.has_books():
                logger.info(f'No books found on page {page}')
                return [], False

            book_links = list_parser.extract_book_links()
            if not book_links:
                return [], False

            has_next_page = list_parser.has_next_page(page)

            books = await self._parse_books_batch(book_links)

            return books, has_next_page
            
        except Exception as e:
            logger.error(f'Error loading {curr_url}: {e}')
            return [], False

    async def _parse_books_batch(self, book_links: List[str]) -> List[Book]:
        tasks = [self._parse_single_book(link) for link in book_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        books = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f'Error in batch: {result}')
            elif result is not None:
                books.append(result)

        return books

    async def _parse_single_book(self, url: str) -> Optional[Book]:
        async with self.semaphore:
            try:
                full_url = SERVICE + url
                html = await self.http_client.get(full_url)
                
                if not html:
                    return None

                loop = asyncio.get_event_loop()
                book = await loop.run_in_executor(
                    self.executor,
                    lambda: BookParser(html, url).parse()
                )
                
                return book
                
            except Exception as e:
                logger.error(f'Error parsing book {url}: {e}', exc_info=True)
                return None