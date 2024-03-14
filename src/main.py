# Originally inspired by https://github.com/suewoon/nyt-api-wrapper/blob/2508718e8fcf6bc8dcd63f5e8874b80684177259/scraper.py

# %%
import pandas as pd
import argparse
import requests
import datetime
import dateutil
from typing import Union, Callable
import traceback
import asyncio
import json
from constants import NYT_API_KEY, END_OF_QUEUE, JSON_LIST, JSON_ITEM, NYT_API_RATE_LIMIT, WAYBACK_API_RATE_LIMIT, OPENAI_API_RATE_LIMIT, FINAL_DIR
import label
import scrape
from savequeue import SaveQueue

QUEUE_OBJECT = Union[asyncio.Queue, SaveQueue]
# %%

async def main(search_params: JSON_LIST):
    search_queue = asyncio.Queue()
    metadata_queue = SaveQueue()
    article_content_queue = SaveQueue()
    llm_labeling_queue = SaveQueue()

    for params in search_params:
        await search_queue.put(params)
    await search_queue.put(END_OF_QUEUE)
    
    process_tasks = [
        forward_queue_items(
            search_queue,
            metadata_queue,
            scrape.get_nyt_article_metadata,
            max_concurrent_calls=NYT_API_RATE_LIMIT,
        ),
        forward_queue_items(
            metadata_queue,
            article_content_queue,
            scrape.get_AI_article_content,
            max_concurrent_calls=WAYBACK_API_RATE_LIMIT,
            ),
        forward_queue_items(
            article_content_queue,
            llm_labeling_queue,
            label.get_content_labels,
            max_concurrent_calls=OPENAI_API_RATE_LIMIT,
        ),
    ]
    snapshot_tasks = [
        metadata_queue.snapshot(filename='metadata'),
        article_content_queue.snapshot(filename='content'),
        llm_labeling_queue.snapshot(filename='labels'),
    ]

    await asyncio.gather(*process_tasks, *snapshot_tasks)
    return metadata_queue.registry, article_content_queue.registry, llm_labeling_queue.registry
    
async def forward_queue_items(
    queue_in: QUEUE_OBJECT,
    queue_out: QUEUE_OBJECT,
    forward_function: Callable[[JSON_ITEM], JSON_LIST],
    max_concurrent_calls: int,
) -> None:
    
    async def process_item(item: JSON_ITEM):
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

    queue_in_running = True
    while queue_in_running:
        items = []
        for _ in range(max_concurrent_calls):
            # if not queue_in.empty():
            item = await queue_in.get()
            if item is END_OF_QUEUE:
                queue_in_running = False
                queue_in.task_done()
                break
            else:
                items.append(item)
                queue_in.task_done()
            # else:
            #     break
                        
        if items:
            await asyncio.gather(*[process_item(item) for item in items])
    
    await queue_out.put(END_OF_QUEUE)
    
    
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
        
def merge_json_data(
    metadata: JSON_LIST,
    content: JSON_LIST,
    labels: JSON_LIST,
) -> pd.DataFrame:
    metadata = pd.DataFrame(metadata)
    content = pd.DataFrame(content)
    labels = pd.DataFrame(labels)
    
    metadata_and_content = metadata.merge(
        content,
        left_on='_id',
        right_on='article_id',
        how='outer',
    )
    metadata_and_content.astype({'text_id': 'Int64'})
    full_data = metadata_and_content.merge(
        labels,
        left_on=['article_id', 'text_id'],
        right_on=['article_id', 'text_id'],
        how='outer',
    )
    full_data = full_data.dropna(subset=['text', 'sentiment'])
    return full_data
        
# %%        

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(prog='', description='cli argument for scarping articles')
    arg_parser.add_argument('--start_date', dest='start_date', help='Date query articles start from (%Y%m)', required=True)
    arg_parser.add_argument('--end_date', dest='end_date', help='Date query articles end (%Y%m)', required=True)
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
    
    save_json(metadata, f'{FINAL_DIR}/metadata-test.json')
    save_json(content, f'{FINAL_DIR}/content-test.json')
    save_json(labels, f'{FINAL_DIR}/labels-test.json')
    
    merged_data = merge_json_data(metadata, content, labels)
    merged_data.to_json(f'{FINAL_DIR}/ai-mood-test.json', orient='records', indent=2)


# %%
