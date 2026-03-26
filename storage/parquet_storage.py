import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Set, Any, Dict
from pathlib import Path

from models.book import Book
from logging_config import logger


class ParquetStorage:
    def __init__(self, filename: str = 'books.parquet'):
        self.filename = filename
        self._known_urls: Set[str] = set()
        self._cache_loaded = False
        self._schema = self._create_schema()

    def _create_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field('name', pa.string()),
            pa.field('url', pa.string()),
            pa.field('author', pa.string()),
            pa.field('new_price', pa.float64()),
            pa.field('old_price', pa.float64()),
            pa.field('discount', pa.int32()),
            pa.field('in_stock', pa.bool_()),
            pa.field('availability_status', pa.string()),
            pa.field('genre', pa.string()),
            pa.field('subgenre', pa.string()),
            pa.field('audience', pa.string()),
            pa.field('subject', pa.string()),
            pa.field('annotation', pa.string()),
            pa.field('publisher', pa.string()),
            pa.field('series', pa.string()),
            pa.field('section', pa.string()),
            pa.field('bookbinding', pa.string()),
            pa.field('number_of_pages', pa.int32()),
            pa.field('year_of_publication', pa.int32()),
            pa.field('edition', pa.int32()),
            pa.field('size', pa.string()),
            pa.field('weight', pa.float64()),
            pa.field('rating', pa.float64()),
            pa.field('review_count', pa.int32()),
        ])

    def _convert_to_correct_type(self, value: Any, field_type: str) -> Any:
        if value is None:
            return None
            
        if field_type in ('int32', 'int64'):
            if isinstance(value, str):
                cleaned = ''.join(filter(str.isdigit, str(value)))
                if cleaned:
                    return int(cleaned)
                return None
            if isinstance(value, (int, float)):
                return int(value)
            return None
            
        elif field_type in ('float64', 'float32'):
            if isinstance(value, str):
                cleaned = ''.join(c for c in str(value) if c.isdigit() or c in '.-')
                if cleaned:
                    return float(cleaned)
                return None
            if isinstance(value, (int, float)):
                return float(value)
            return None
            
        elif field_type == 'bool_':
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'в наличии')
            return bool(value)
            
        return value

    def _book_to_dict(self, book: Book) -> dict:
        data = book.to_dict()
        
        result = {}
        field_types = {
            'name': 'string',
            'url': 'string',
            'author': 'string',
            'new_price': 'float64',
            'old_price': 'float64',
            'discount': 'int32',
            'in_stock': 'bool_',
            'availability_status': 'string',
            'genre': 'string',
            'subgenre': 'string',
            'audience': 'string',
            'subject': 'string',
            'annotation': 'string',
            'publisher': 'string',
            'series': 'string',
            'section': 'string',
            'bookbinding': 'string',
            'number_of_pages': 'int32',
            'year_of_publication': 'int32',
            'edition': 'int32',
            'size': 'string',
            'weight': 'float64',
            'rating': 'float64',
            'review_count': 'int32',
        }
        
        for pyarrow_field, field_type in field_types.items():
            dict_key = pyarrow_field.replace('_', ' ')
            value = data.get(dict_key)

            converted = self._convert_to_correct_type(value, field_type)
            result[pyarrow_field] = converted
            
        return result

    def load_known_urls(self):
        if self._cache_loaded:
            return

        try:
            if Path(self.filename).exists():
                table = pq.read_table(self.filename, columns=['url'])
                self._known_urls = set(table.column('url').to_pylist())
                logger.info(f'Loaded {len(self._known_urls)} known URLs from parquet cache.')
        except Exception as e:
            logger.error(f'Error loading known URLs: {e}')
            self._known_urls = set()
        finally:
            self._cache_loaded = True

    def save_books(self, books: List[Book]) -> int:
        try:
            self.load_known_urls()

            new_books = [book for book in books if book.url not in self._known_urls]

            if not new_books:
                logger.info('No new books to save')
                return 0

            new_data = [self._book_to_dict(book) for book in new_books]
            new_table = pa.Table.from_pylist(new_data, schema=self._schema)

            if Path(self.filename).exists():
                existing_table = pq.read_table(self.filename)
                combined_table = pa.concat_tables([existing_table, new_table])
            else:
                combined_table = new_table

            pq.write_table(
                combined_table,
                self.filename,
                compression='SNAPPY',
                row_group_size=100000
            )

            for book in new_books:
                self._known_urls.add(book.url)

            logger.info(f'Added {len(new_books)} new books to Parquet file. '
                       f'Skipped {len(books) - len(new_books)} duplicates. '
                       f'Total books: {len(self._known_urls)}')
            
            return len(new_books)

        except Exception as e:
            logger.error(f'Error saving to Parquet file: {e}')
            return 0

    def get_known_urls(self) -> Set[str]:
        if not self._cache_loaded:
            self.load_known_urls()
        return self._known_urls.copy()

    def read_all(self) -> pa.Table:
        if Path(self.filename).exists():
            return pq.read_table(self.filename)
        return pa.Table.from_pylist([], schema=self._schema)

    def get_stats(self) -> dict:
        stats = {
            'filename': self.filename,
            'total_books': len(self._known_urls),
            'cache_loaded': self._cache_loaded
        }
        
        if Path(self.filename).exists():
            file_size = Path(self.filename).stat().st_size
            stats['file_size_mb'] = file_size / (1024 * 1024)
            
            try:
                metadata = pq.read_metadata(self.filename)
                stats['num_rows'] = metadata.num_rows
                stats['num_columns'] = metadata.num_columns
                stats['num_row_groups'] = metadata.num_row_groups
                stats['compression'] = 'SNAPPY'
            except:
                pass

        return stats