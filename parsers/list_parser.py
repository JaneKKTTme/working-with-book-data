import re
from bs4 import BeautifulSoup

from logging_config import logger


class ListParser:

    def __init__(self, html: str, page: int):
        self.soup = BeautifulSoup(html, 'lxml')
        self.page = page

    def extract_book_links(self) -> list:
        book_links = []
        
        for book_card in self.soup.find_all('a', attrs={'class': 'product-card__image-link base-link'}):
            href = book_card.get('href')
            if href:
                book_links.append(href)

        if not book_links:
            logger.warning(f'No book links found on page {self.page}')
            
        return book_links

    def has_next_page(self, current_page: int) -> bool:
        next_buttons = self.soup.find_all('a', string=re.compile(r'Далее|Следующая|Вперед|>', re.I))
        if next_buttons:
            return True

        pagination_links = self.soup.find_all('a', href=True)
        for link in pagination_links:
            href = link.get('href', '')
            if f'page={current_page + 1}' in href:
                return True

        return False

    def has_books(self) -> bool:
        container = self.soup.find('div', attrs={'class': 'product-list app-catalog__products'})
        return container is not None