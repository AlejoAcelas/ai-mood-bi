import requests
import ratelimit
import re
import parse
from constants import (
    NYT_ENDPOINT,
    WAYBACK_ENDPOINT,
    NYT_METADATA_COLUMNS,
    JSON_ITEM,
    JSON_LIST,
    ONE_MINUTE,
    NYT_API_RATE_LIMIT,
    WAYBACK_API_RATE_LIMIT,
    )

@ratelimit.sleep_and_retry
@ratelimit.limits(calls=NYT_API_RATE_LIMIT, period=ONE_MINUTE)
async def get_nyt_article_metadata(params: JSON_ITEM) -> JSON_LIST:
    response = requests.get(NYT_ENDPOINT, params=params).json()
    metadata = []
    if response:
        # print(response)
        for row in response['response']['docs']:
            row_metadata = {col: row[col] for col in NYT_METADATA_COLUMNS}
            
            row_metadata['keywords'] = [kw_info['value'] for kw_info in row['keywords']]
            row_metadata['date_batch_start'] = params['begin_date']
            row_metadata['date_batch_end'] = params['end_date']
            row_metadata['page'] = params['page']
            
            metadata.append(row_metadata)
        return metadata
    else:
        return []

@ratelimit.sleep_and_retry
@ratelimit.limits(calls=WAYBACK_API_RATE_LIMIT, period=ONE_MINUTE)
async def get_AI_article_content(nyt_metadatum: JSON_ITEM) -> JSON_LIST:
    url = nyt_metadatum['web_url']
    response = requests.get(WAYBACK_ENDPOINT, params={'url':url}).json()
    if response['archived_snapshots']:
        try:
            wayback_url = response['archived_snapshots']['closest']['url']
            page = requests.get(wayback_url)
            
            body_paragraphs = parse.extract_nyt_body_paragraphs(page.content)
            AI_paragraphs = [p for p in body_paragraphs if is_AI_related(p)]
        except Exception as e:
            print(e)
            AI_paragraphs =  ["NULL: Couldn't access wayback machine"]
    else:
        AI_paragraphs = ["NULL: Article not found in wayback machine"]
    
    out_json = [{'article_id': nyt_metadatum['_id'], 'text_id': i, 'text': paragraph} 
                for i, paragraph in enumerate(AI_paragraphs)]
    return out_json

def is_AI_related(paragraph: str) -> bool:
    keywords = ['artificial intelligence', 'robot', 'ai']
    pattern = r'\b(' + '|'.join(keywords) + r')\b'
    return bool(re.search(pattern, paragraph.lower()))

