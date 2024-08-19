import aiohttp
import asyncio
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    Url,
    Domain,
)

class DataCache:
    """
    A simple class to cache fetched items and provide them to the query function.
    """
    def __init__(self):
        self.cache = []

    def add_items(self, items):
        self.cache.extend(items)

    def pop_item(self):
        if self.cache:
            return self.cache.pop(0)
        return None

    def is_empty(self):
        return len(self.cache) == 0

data_cache = DataCache()

async def request_data(batch_size: int):
    """
    Request data from the server with the specified batch size.
    """
    params = {"count": batch_size}  # Send the batch size as a query parameter
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://192.227.159.6:8080/fetch', params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 204:
                        print("No content available. Backing off for 10 seconds...")
                        await asyncio.sleep(10)
                    else:
                        print(f"Failed to fetch data: {response.status}")
                        return []
        except aiohttp.ClientConnectorError:
            print("Connection error. Retrying in 10 seconds...")
            await asyncio.sleep(10)


def format_item(data: dict) -> Item:
    """
    Format the data received into the Item class as per the requirements.
    """
    item = Item(
        content=Content(data["content"]),
        author=Author(data["author"]),
        created_at=CreatedAt(data["created_at"]),
        domain=Domain(data["domain"]),
        external_id=ExternalId(data["external_id"]),
        url=Url(data["url"]),
    )
    return item

async def scrape(batch_size: int):
    """
    The main scraping logic that fetches data and processes it into the required format.
    """
    fetched_items = await request_data(batch_size)
    formatted_items = [format_item(item_data) for item_data in fetched_items]
    return formatted_items

async def query(parameters: dict):
    """
    The main interface function that yields Items to the client core.
    """
    batch_size = parameters.get("batch_size", 200)  # Default to 10 if not provided

    while True:
        if data_cache.is_empty():
            items = await scrape(batch_size)
            data_cache.add_items(items)

        item = data_cache.pop_item()
        if item:
            yield item
