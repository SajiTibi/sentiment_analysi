from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from timeit import default_timer as timer
from typing import List

import requests


def contains_phrase(phrase: str, headline: str) -> bool:
    return phrase.lower() in headline.lower()


def get_kids(topic_id: str, phrase: str) -> List[str]:
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{topic_id}.json"
    data = requests.get(topic_url).json()
    if contains_phrase(phrase, data["title"]):
        return data["kids"] if "kids" in data else []
    return []


def get_comment(comment_id: str) -> str:
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{comment_id}.json"
    data = requests.get(topic_url).json()
    return data["text"] if "text" in data else None


def collect(phrase: str) -> List[str]:
    top_topics_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    data = requests.get(top_topics_url).json()
    comments_ids = []
    s = timer()

    with ThreadPoolExecutor()as pool:
        for topic_kids in (list(pool.map(get_kids, data, repeat(phrase)))):
            if topic_kids:
                comments_ids.append(topic_kids)
    e1 = timer() - s
    comments_ids = sum(comments_ids, [])
    print(f'took {e1} and got {len(comments_ids)} comments')

    with ThreadPoolExecutor()as pool:
        comments_txt = [comment for comment in (list(pool.map(get_comment, comments_ids))) if
                        comment and len(comment) < 5000]
    e2 = timer() - s - e1
    print(f' took {e2} and got {len(comments_txt)} comments_txt')
    return comments_txt


