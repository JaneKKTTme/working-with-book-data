import asyncio
import argparse

from core.parser import BookvoedParser
from logging_config import logger


async def main():
    parser = argparse.ArgumentParser(description='Parser for bookvoed.ru')
    parser.add_argument('--start-page', type=int, default=1,
                       help='Starting page number')
    parser.add_argument('--concurrent', type=int, default=30,
                       help='Number of concurrent tasks')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay between requests')
    
    args = parser.parse_args()
    
    bookvoed_parser = None
    try:
        bookvoed_parser = BookvoedParser(
            max_concurrent_tasks=args.concurrent,
            delay=args.delay
        )
        
        async with bookvoed_parser:
            await bookvoed_parser.parse_bookvoed(start_page=args.start_page)
            
    except KeyboardInterrupt:
        logger.info('Received interrupt signal.')
    except Exception as e:
        logger.error(f'Fatal error: {e}', exc_info=True)
    finally:
        if bookvoed_parser:
            await bookvoed_parser.close()
        logger.info('Parser finished')


if __name__ == '__main__':
    asyncio.run(main())