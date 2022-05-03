import module as md
import ssl
import certifi


async def scrape_api(url):
    async with semaphore:
        try:
            async with session.get(url) as rsp:
                return await rsp.json()
        except md.aiohttp.ClientError:
            md.logging.error("error: %s", url, exc_info=True)


async def scrape_page(page):
    url = INDEX_URL.format(offset=PAGE_SIZE * (page - 1))
    return await scrape_api(url)


async def main():
    global session
    session = md.aiohttp.ClientSession(connector=md.aiohttp.TCPConnector(limit=64, ssl=False))

    scrape_index_tasks = [md.asyncio.ensure_future(scrape_page(page)) for page in
                          range(1, PAGE_NUMBER + 1)]
    results = await md.asyncio.gather(*scrape_index_tasks)

    # 爬取详情页
    ids = list()
    for index_data in results:
        if not index_data:
            continue
        for item in index_data.get('results'):
            ids.append(item.get('id'))

    scrape_detail_tasks = [md.asyncio.ensure_future(scrape_detail(id)) for id in ids]
    await md.asyncio.wait(scrape_detail_tasks)
    await session.close()


async def save_data(data):
    if data:
        return await collection.update_one({'id': data.get('id')}, {'$set': data}, upsert=True)


async def scrape_detail(id):
    url = DEFAULT_URL.format(id=id)
    data = await scrape_api(url)
    await save_data(data)


if __name__ == '__main__':
    INDEX_URL = "https://spa5.scrape.center/api/book/?limit=18&offset={offset}"
    DEFAULT_URL = "https://spa5.scrape.center/api/book/{id}"
    MONGO_CONNECTION_STRING = "mongodb://localhost:27017"
    MONGO_DB_NAME = "spider"
    MONGO_COLLECTION_NAME = "books"
    PAGE_SIZE = 18
    PAGE_NUMBER = 100
    CONCURRENCY = 5

    client = md.AsyncIOMotorClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    st = md.time.time()
    conn = md.aiohttp.TCPConnector(ssl=False)  # 防止ssl报错
    semaphore = md.asyncio.Semaphore(CONCURRENCY)

    md.asyncio.get_event_loop().run_until_complete(main())

    print(md.time.time() - st)
print('my name is twz')
