# Based on https://github.com/suewoon/nyt-api-wrapper/blob/2508718e8fcf6bc8dcd63f5e8874b80684177259/scraper.py

# %%
import argparse
import requests
import datetime
import dateutil
import pandas as pd 
from typing import Union, Callable
import traceback
import asyncio
import json
from constants import NYT_API_KEY, END_OF_QUEUE, JSON_LIST, JSON_ITEM
import label
import scrape

QUEUE_OBJECT = Union[asyncio.Queue, 'SaveQueue']
# %%

class SaveQueue():
    def __init__(self):
        self.queue = asyncio.Queue()
        self.registry: JSON_LIST = []
        self.done: bool = False
        
    async def put(self, item):
        if item == END_OF_QUEUE:
            self.done = True
        else:
            self.registry.append(item)
        await self.queue.put(item)
    
    async def get(self):
        return await self.queue.get()
    
    def task_done(self):
        self.queue.task_done()
        
    async def snapshot(self, filename: str, interval: int = 15):
        while not self.done:
            self.save_registry(filename)
            await asyncio.sleep(interval)

    def save_registry(self, filename: str):
        with open(f"test/{filename}.json", 'w') as file:
            json.dump(self.registry, file, indent=2)
        self.log_registry(filename)
        
    def log_registry(self, filename: str):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(f'test/log/{filename}.txt', 'a') as file:
            file.write(f"{current_time}: Saved {len(self.registry)} items from registry\n")

async def main(search_params: JSON_LIST):
    search_queue = asyncio.Queue()
    metadata_queue = SaveQueue()
    article_content_queue = SaveQueue()
    llm_labeling_queue = SaveQueue()

    for params in search_params:
        await search_queue.put(params)
    await search_queue.put(END_OF_QUEUE)
    
    nyt_task = asyncio.create_task(
        forward_queue_items(search_queue, metadata_queue, scrape.get_nyt_article_metadata)
    )
    wayback_task = asyncio.create_task(
        forward_queue_items(metadata_queue, article_content_queue, scrape.get_AI_article_content)
    )
    llm_task = asyncio.create_task(
        forward_queue_items(article_content_queue, llm_labeling_queue, label.get_content_labels)
    )

    await nyt_task
    await wayback_task
    await llm_task
    
    await metadata_queue.snapshot('metadata')
    await article_content_queue.snapshot('content')
    await llm_labeling_queue.snapshot('labels')
    
    return metadata_queue.registry, article_content_queue.registry, llm_labeling_queue.registry
    

async def forward_queue_items(
    queue_in: QUEUE_OBJECT,
    queue_out: QUEUE_OBJECT,
    forward_function: Callable[[JSON_ITEM], JSON_LIST]
) -> None:
    while True:
        item = await queue_in.get()
        
        if item is END_OF_QUEUE:
            await queue_out.put(END_OF_QUEUE)
            break
        try:
            items_out = await forward_function(item)
            for item_out in items_out:
                await queue_out.put(item_out)
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while calling {forward_function.__name__} with {item}: {e}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"Error occurred while processing {item} with {forward_function.__name__}: {e}")
            print(traceback.format_exc())
        
        queue_in.task_done()        

    
def generate_date_and_page_params(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    articles_per_month: int,
) -> JSON_LIST:
    param_list = []
    articles_per_page = 10
    one_month = dateutil.relativedelta.relativedelta(months=1)
    one_day = dateutil.relativedelta.relativedelta(days=1)
    
    month_start = start_date
    while month_start < end_date:
        month_end = month_start + one_month - one_day
        for page_num in range(articles_per_month//articles_per_page):
            params = {
                'begin_date': month_start.strftime('%Y%m%d'),
                'end_date': month_end.strftime('%Y%m%d'),
                'page': page_num
            }
            param_list.append(params)
        month_start = month_end + one_day
    return param_list

def save_json(json_object, filename):
    with open(filename, 'w') as file:
        json.dump(json_object, file, indent=2)
        
if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(prog='', description='cli argument for scarping articles')
    # arg_parser.add_argument('--start_date', dest='start_date', help='Date query articles start from (%Y%m)', required=True)
    # arg_parser.add_argument('--end_date', dest='end_date', help='Date query articles end (%Y%m)', required=True)
    arg_parser.add_argument('--start_date', dest='start_date', help='Date query articles start from (%Y%m)', default='202101')
    arg_parser.add_argument('--end_date', dest='end_date', help='Date query articles end (%Y%m)', default='202102')
    # arg_parser.add_argument('--filename', dest='filename', help='Filename to save the data', required=True)
    arg_parser.add_argument('--articles_per_month', dest='articles_per_month', default=10, help='Number of NYT articles to scrape from every month')
    args = arg_parser.parse_args()

    start_date = datetime.datetime.strptime(args.start_date, "%Y%m")
    end_date = datetime.datetime.strptime(args.end_date, "%Y%m")
    articles_per_month = int(args.articles_per_month)
    
    fixed_search_params = {
        'q': 'artificial intelligence',
        'fq': """document_type: ("article")""",
        'sort': 'relevance',
        'api-key': NYT_API_KEY,
    }
    variable_search_params = generate_date_and_page_params(start_date, end_date, articles_per_month)
    search_param_list = [{**fixed_search_params, **params} for params in variable_search_params]
    metadata, content, labels = asyncio.run(main(search_param_list))
    
    save_json(metadata, 'data/metadata-test.json')
    save_json(content, 'data/content-test.json')
    save_json(labels, 'data/labels-test.json')

