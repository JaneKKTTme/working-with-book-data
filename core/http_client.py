import asyncio
import random
import ssl
import time
from collections import deque
from http import HTTPStatus
from typing import Optional

import aiohttp

from config import HEADERS, USER_AGENTS, REQUEST_CONFIG, CONNECTOR_CONFIG
from logging_config import logger


class HTTPClient:

	def __init__(self, max_retries: int = REQUEST_CONFIG['max_retries'],
			timeout: int = REQUEST_CONFIG['timeout'],
			delay: float = REQUEST_CONFIG['delay_between_requests']):
		self.max_retries = max_retries
		self.timeout = timeout
		self.delay = delay
		self.request_times = deque(maxlen=60)
		self.shutdown_event = asyncio.Event()
		self.session: Optional[aiohttp.ClientSession] = None
		self._connector = None
		self.ssl_context = self._create_ssl_context()

	def _create_ssl_context(self):
		context = ssl.create_default_context()
		context.check_hostname = False
		context.verify_mode = ssl.CERT_NONE
		return context

	async def __aenter__(self):
		self.connector = aiohttp.TCPConnector(**CONNECTOR_CONFIG)
		self.session = aiohttp.ClientSession(connector=self.connector)
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self.close()

	async def get(self, url: str) -> Optional[str]:
		await self._adaptive_delay()

		if not self.session:
			self.session = aiohttp.ClientSession()

		timeout = aiohttp.ClientTimeout(total=self.timeout)

		for attempt in range(self.max_retries):
			try:
				headers = HEADERS.copy()
				headers['User-Agent'] = random.choice(USER_AGENTS)
				headers['Accept-Encoding'] = 'gzip, deflate, br'
				async with self.session.get(
				  url,
				  ssl=self.ssl_context,
				  timeout=timeout,
				  headers=headers
				) as response:
					if response.status == HTTPStatus.TOO_MANY_REQUESTS:
						retry_after = response.headers.get('Retry-After', self.timeout)
						wait_time = int(retry_after)
						logger.warning(f'Rate limited. Waiting {wait_time} seconds...')
						await asyncio.sleep(wait_time)
						response.raise_for_status()

					if response.status != HTTPStatus.OK:
						logger.error(f'HTTP error {response.status} for {url}')
						return None

					return await response.text()

			except aiohttp.ClientResponseError as e:
				if e.status == HTTPStatus.TOO_MANY_REQUESTS:
					logger.error(f'Rate limit exceeded for {url}. Increasing delays...')
					self.delay = min(self.delay * 1.5, 10)
					continue
				raise

			except (aiohttp.ClientError, asyncio.TimeoutError) as e:
				if attempt == self.max_retries - 1:
					raise

				if isinstance(e, aiohttp.ClientConnectionError):
					if self.session:
						await self.session.close()
					self.session = aiohttp.ClientSession()

				wait_time = 2 ** attempt
				logger.error(f'Request failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}')
				await asyncio.sleep(wait_time)

	async def _adaptive_delay(self) -> None:
		now = time.time()
		self.request_times.append(now)

		while self.request_times and now - self.request_times[0] > 60:
			self.request_times.popleft()

		requests_per_second = len(self.request_times) / 60

		if requests_per_second > 0.8:
			delay = self.delay * 1.5
		elif requests_per_second > 0.5:
			delay = self.delay * 1.2
		else:
			delay = self.delay

		await asyncio.sleep(delay)

	async def close(self):
		if self.session:
			await self.session.close()
		if self._connector:
			await self._connector.close()

	async def shutdown(self):
		logger.info('Shutting down gracefully...')
		self.shutdown_event.set()