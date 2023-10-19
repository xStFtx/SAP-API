import os
import base64
import logging.config
import configparser
import aiohttp
import async_timeout
import time

from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer

# Read Configurations
config = configparser.ConfigParser()
config.read('settings.ini')

# Setup Logging
logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

class AuthenticationError(Exception):
    pass

class DataFetchError(Exception):
    pass

class RateLimitError(Exception):
    pass

class SAP_ODataService:
    def __init__(self):
        self.service_url = config.get('SAP', 'service_url')
        self.headers = {
            "Accept": "application/json",
            "Authorization": self.generate_basic_auth_header()
        }
        self.rate_limit_reset = 0
        self.session = aiohttp.ClientSession()

    def generate_basic_auth_header(self):
        username = os.environ.get('SAP_USERNAME')
        password = os.environ.get('SAP_PASSWORD')
        if not username or not password:
            raise AuthenticationError("Missing SAP_USERNAME or SAP_PASSWORD environment variables.")
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"

    async def close(self):
        await self.session.close()

    @cached(ttl=300, cache=Cache.MEMORY, serializer=JsonSerializer())
    async def make_request(self, url, method="GET", params=None, json=None, retries=3):
        if time.time() < self.rate_limit_reset:
            raise RateLimitError("Too many requests. Please wait.")

        try:
            with async_timeout.timeout(10):
                async with self.session.request(method, url, headers=self.headers, params=params, json=json) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 0))
                        self.rate_limit_reset = time.time() + retry_after
                        raise RateLimitError(f"Rate limit hit. Retry after {retry_after} seconds.")
                    
                    if response.status != 200:
                        if retries:
                            await asyncio.sleep(1)  # Wait for a second before retry
                            return await self.make_request(url, method, params, json, retries-1)
                        else:
                            self.handle_errors(response)
                            return None
                    
                    return await response.json()

        except Exception as e:
            logger.error(f"Error making request: {e}")
            raise DataFetchError(f"Error fetching data from {url}")

    async def get_data(self, endpoint, params=None):
        url = f"{self.service_url}/{endpoint}"
        return await self.handle_pagination(url, params)

    async def handle_pagination(self, url, params=None):
        data = await self.make_request(url, params=params)
        while "@odata.nextLink" in data:
            next_page_data = await self.make_request(data["@odata.nextLink"])
            data['value'].extend(next_page_data['value'])
            data["@odata.nextLink"] = next_page_data.get("@odata.nextLink")
        return data

    async def create_data(self, endpoint, json):
        url = f"{self.service_url}/{endpoint}"
        return await self.make_request(url, method="POST", json=json)

    async def update_data(self, endpoint, json):
        url = f"{self.service_url}/{endpoint}"
        return await self.make_request(url, method="PUT", json=json)

    async def delete_data(self, endpoint):
        url = f"{self.service_url}/{endpoint}"
        return await self.make_request(url, method="DELETE")

    def handle_errors(self, response):
        if response.status == 400:
            logger.error("Bad Request - Missing parameters.")
        elif response.status == 401:
            logger.error("Unauthorized - Authentication failed.")
        elif response.status == 403:
            logger.error("Forbidden - User does not have access.")
        elif response.status == 404:
            logger.error("Not Found - Resource not found.")
        elif response.status == 500:
            logger.error("Internal Server Error.")
        else:
            logger.error(f"HTTP Error {response.status}.")

# Usage with asyncio
import asyncio

async def main():
    sap_service = SAP_ODataService()
    try:
        data = await sap_service.get_data("entity")
        if data:
            for item in data['value']:
                print(item)
    except (AuthenticationError, DataFetchError, RateLimitError) as e:
        logger.error(e)
    finally:
        await sap_service.close()

asyncio.run(main())
