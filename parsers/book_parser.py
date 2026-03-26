import re
from bs4 import BeautifulSoup

from models.book import Book
from parsers.json_ld_parser import JsonLdParser
from logging_config import logger


class BookParser:

    CHAR_MAPPING = {
        'автор': 'author',
        'поджанр': 'subgenre',
        'аудитория': 'audience',
        'тематика': 'subject',
        'издательство': 'publisher',
        'серия': 'series',
        'переплет': 'bookbinding',
        'кол-во страниц': 'number_of_pages',
        'раздел': 'section',
        'размеры': 'size',
        'вес': 'weight',
        'тираж': 'edition',
    }

    def __init__(self, html: str, url: str):
        self.soup = BeautifulSoup(html, 'lxml')
        self.url = url
        self.book = Book()
        self.book.url = url

    def parse(self) -> Book:
        try:
            self._parse_book_name()
            JsonLdParser.parse(self.soup, self.book)
            self._parse_prices()
            self._parse_availability()
            self._parse_characteristics()

            return self.book
        except Exception as e:
            logger.error(f'Error parsing book {self.url}: {e}', exc_info=True)
            return None

    def _parse_book_name(self):
        name_tag = self.soup.find('h1', attrs={'class': 'product-title-author__title'})
        self.book.name = name_tag.get_text(strip=True) if name_tag else ''

    def _parse_prices(self):
        price_block = self.soup.find('div', attrs={'class': 'price-block-price-info'})
        if price_block:
            price_container = price_block.find('div', attrs={'class': 'price-block-price-info__price'})
            if price_container:
                spans = price_container.find_all('span')

                if len(spans) >= 1:
                    new_price = self._extract_numbers(spans[0].get_text(strip=True))
                    if new_price:
                        self.book.new_price = float(''.join(new_price))

                if len(spans) >= 2:
                    old_price = self._extract_numbers(spans[1].get_text(strip=True))
                    if old_price:
                        self.book.old_price = float(''.join(old_price))

            discount = price_block.find('div', attrs={'class': 'price-block-price-info__discount'})
            if discount:
                discount_text = discount.get_text(strip=True)
                discount_match = re.search(r'(\d+)', discount_text)
                if discount_match:
                    self.book.discount = int(discount_match.group(1))

    def _parse_availability(self):
        availability_block = self.soup.find('div', attrs={'class': 'price-block-availability '
                                                              'order-info-price-block__availability'})
        if not availability_block:
            availability_block = self.soup.find('div', attrs={'class': 'price-block-preorder'})
        
        if availability_block:
            availability_text = availability_block.get_text(strip=True)
            self.book.availability_status = availability_text

            text_lower = availability_text.lower()
            self.book.in_stock = any(word in text_lower for word in ['в наличии', 'осталось мало'])

    def _parse_characteristics(self):
        characteristics = self._extract_characteristics()

        for key, value in characteristics.items():
            mapped_key = self.CHAR_MAPPING.get(key.lower(), key)
            processed_value = self._process_value(mapped_key, value)
            setattr(self.book, mapped_key, processed_value)

    def _extract_characteristics(self) -> dict:
        characteristics = {}

        full_char_table = self.soup.find('div', class_='product-characteristics-full')
        if full_char_table:
            rows = full_char_table.find_all('tr', class_='product-characteristics-full__row')
            for row in rows:
                th = row.find('th', class_='product-characteristics-full__cell-th')
                td = row.find('td', class_='product-characteristics-full__cell-td')

                if th and td:
                    key = self._normalize_text(th.get_text(strip=True))
                    value = self._extract_value(td)
                    
                    if key and value:
                        characteristics[key] = value
        return characteristics

    def _extract_value(self, html_block):
        list_items = html_block.find_all('li')
        if list_items:
            return ';'.join(item.get_text(strip=True).replace(',', '') for item in list_items)
        return html_block.get_text(strip=True)

    def _process_value(self, key: str, value: str):
        if key == 'weight':
            match = re.search(r'(\d+(?:\.\d+)?)', value)
            return float(match.group(1)) if match else None
        elif key == 'edition':
            match = re.search(r'(\d+(?:\xA0\d+)?)', value)
            return int(match.group(1).replace('\xA0', '')) if match else None
        return value

    @staticmethod
    def _extract_numbers(text: str) -> list:
        return re.findall(r'\d+', text)

    @staticmethod
    def _normalize_text(text: str) -> str:
        return ' '.join(text.lower().split())