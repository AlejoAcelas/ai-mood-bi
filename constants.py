import os
from typing import List, Dict

### APIs
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
NYT_API_KEY = os.environ['NYT_API_TOKEN']

NYT_ENDPOINT = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
WAYBACK_ENDPOINT = 'http://archive.org/wayback/available'

NYT_API_RATE_LIMIT = 4 # requests per minute
WAYBACK_API_RATE_LIMIT = 14 # requests per minute
OPENAI_API_RATE_LIMIT = 50 # requests per minute

ONE_MINUTE = 60 # seconds
SNAPSHOT_INTERVAL = 60 # seconds

### Typing
JSON_LIST = List[Dict[str, str]]
JSON_ITEM = Dict[str, str]

### Other
NYT_METADATA_COLUMNS = ['_id', 'web_url', 'pub_date', 'document_type', 'type_of_material', 'word_count', 'keywords', 'source']
END_OF_QUEUE = 'END_OF_QUEUE'
