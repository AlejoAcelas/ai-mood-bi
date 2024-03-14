# Edited from https://github.com/lxucs/commoncrawl-warc-retrieval/blob/27c23cefa2035d9a16a95aebbd152f12a3bbb9a9/parser-nytimes.py
from bs4 import BeautifulSoup
from typing import Optional, List

def extract_nyt_body_paragraphs(content: str) -> Optional[List[str]]:
    PARSE_FUNCTIONS = [
        select_on_paragraph_property,
        select_on_container_property,
    ]
    soup = BeautifulSoup(content, 'lxml')
    for parse_fn in PARSE_FUNCTIONS:
        try:
            paragraphs = parse_fn(soup)
            if paragraphs and len(paragraphs) > 0:
                return paragraphs
        except Exception as e:
            continue
    return "NULL: No article body found"
    
def select_on_paragraph_property(soup: BeautifulSoup) -> Optional[List[str]]:
    p_properties = [
        {'itemprop': 'articleBody'},
        {'class': 'story-content'},
        {'class': 'story-body-text'},
    ]
    for p_prop in p_properties:
        if soup.find("p", p_prop):
            paragraphs = [p.getText().strip() for p in soup.find_all("p", p_prop)]
            return paragraphs
    return None

def select_on_container_property(soup: BeautifulSoup) -> Optional[List[str]]:
    paragraphs = [p.getText().strip() for p in 
                  soup.select("#story div.StoryBodyCompanionColumn p")]
    return paragraphs
