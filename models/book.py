from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Book:
    name: str = ''
    url: str = ''
    author: str = ''

    new_price: Optional[float] = None
    old_price: Optional[float] = None
    discount: Optional[int] = None

    in_stock: bool = False
    availability_status: str = ''

    genre: str = ''
    subgenre: str = ''
    audience: str = ''
    subject: str = ''
    annotation: str = ''

    publisher: str = ''
    series: str = ''
    section: str = ''
    bookbinding: str = ''
    number_of_pages: Optional[int] = None
    year_of_publication: Optional[int] = None
    edition: Optional[int] = None
    size: str = ''
    weight: Optional[float] = None

    rating: Optional[float] = None
    review_count: Optional[int] = None


    def __init__(self, **kwargs):
        self.name = kwargs.get('name', '')
        self.url = kwargs.get('url', '')
        self.author = kwargs.get('author', '')

        new_price = kwargs.get('new_price', '')
        self.new_price = float(new_price) if new_price else None

        old_price = kwargs.get('old_price', '')
        self.old_price = float(old_price) if old_price else None

        discount = kwargs.get('discount', '')
        self.discount = int(discount) if discount else None

        self.in_stock = kwargs.get('in_stock', False)
        self.availability_status = kwargs.get('availability_status', '')

        self.genre = kwargs.get('genre', '')
        self.subgenre = kwargs.get('subgenre', '')
        self.audience = kwargs.get('audience', '')
        self.subject = kwargs.get('subject', '')
        self.annotation = kwargs.get('annotation', '')

        self.publisher = kwargs.get('publisher', '')
        self.series = kwargs.get('series', '')
        self.section = kwargs.get('section', '')
        self.bookbinding = kwargs.get('bookbinding', '')

        number_of_pages = kwargs.get('number_of_pages', '')
        self.number_of_pages = int(number_of_pages) if number_of_pages else None

        year_of_publication = kwargs.get('year_of_publication', '')
        self.year_of_publication = int(year_of_publication) if year_of_publication else None

        edition = kwargs.get('edition', '')
        self.edition = int(edition) if edition else None

        self.size = kwargs.get('size', '')

        weight = kwargs.get('weight', '')
        self.weight = float(weight) if weight else None

        rating = kwargs.get('rating', '')
        self.rating = float(rating) if rating else None

        review_count = kwargs.get('review_count', '')
        self.review_count = int(review_count) if review_count else None

    def __repr__(self):
        return f'Book(name="{self.name}", author="{self.author}")'

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        book = {}
        for key, value in asdict(self).items():
            key = key.replace('_', ' ')
            book[key] = value
        return book