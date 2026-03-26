import json
from bs4 import BeautifulSoup

from models.book import Book
from logging_config import logger


class JsonLdParser:

    FORMAT_MAP = {
        'https://schema.org/Hardcover': 'Твёрдый переплёт',
        'https://schema.org/Paperback': 'Мягкая обложка'
    }

    @classmethod
    def parse(cls, soup: BeautifulSoup, book: Book) -> Book:
        script_tags = soup.find_all('script', type='application/ld+json')

        for script in script_tags:
            try:
                if not script.string:
                    continue

                data = json.loads(script.string)

                if isinstance(data, dict) and '@graph' in data:
                    for item in data['@graph']:
                        if item.get('@type') == 'Book':
                            cls._extract_book_data(item, book)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.debug(f'Error parsing JSON-LD: {e}')
                continue

        return book

    @classmethod
    def _extract_book_data(cls, item: dict, book: Book):
        if 'description' in item:
            book.annotation = item.get('description', '').replace('\xA0', '')

        if 'genre' in item:
            book.genre = item.get('genre', '')

        if 'bookFormat' in item:
            book.bookbinding = cls.FORMAT_MAP.get(item.get('bookFormat'), item.get('bookFormat', ''))

        if 'numberOfPages' in item:
            book.number_of_pages = item.get('numberOfPages', '')

        if 'publisher' in item:
            book.publisher = item.get('publisher', '')

        if 'datePublished' in item:
            book.year_of_publication = item.get('datePublished', '')

        aggregate_rating = item.get('aggregateRating', {})
        if isinstance(aggregate_rating, dict):
            book.rating = aggregate_rating.get('ratingValue', '')
            book.review_count = aggregate_rating.get('reviewCount', '')