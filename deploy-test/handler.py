import json
import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from itertools import repeat
from timeit import default_timer as timer
from typing import List

import boto3
import matplotlib.pyplot as plt
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
    with ThreadPoolExecutor()as pool:
        for topic_kids in (list(pool.map(get_kids, data, repeat(phrase)))):
            if topic_kids:
                comments_ids.append(topic_kids)
    comments_ids = sum(comments_ids, [])
    with ThreadPoolExecutor()as pool:
        comments_txt = [comment for comment in (list(pool.map(get_comment, comments_ids))) if comment]
    return comments_txt


def get_comments_analysis(comments: List[str]) -> List[dict]:
    n = 25
    comprehend = boto3.client(service_name='comprehend')
    comments_chunks = [comments[i:i + n] for i in range(0, len(comments), n)]
    comments_analysis = []
    for comments_chunk in comments_chunks:
        comments_analysis.append(json.loads(
            json.dumps(comprehend.batch_detect_sentiment(TextList=comments_chunk,
                                                         LanguageCode='en'), sort_keys=True, indent=4))["ResultList"])
    return sum(comments_analysis, [])


def run(phrase: str):
    start = timer()
    comments = collect(phrase)
    comments_analysis = get_comments_analysis(comments)

    counter_d = dict()
    all_count = 0
    for comment in comments_analysis:
        comment_sentiment = comment["Sentiment"]
        if comment_sentiment in counter_d:
            counter_d[comment_sentiment] += 1
        else:
            counter_d[comment_sentiment] = 1
        all_count += 1
    for k in counter_d.keys():
        counter_d[k] = "%.2f" % ((counter_d[k] / all_count) * 100)
    end = timer()
    run_time = "%.2f" % (end - start)
    return {"results": counter_d, "comments_count": all_count, "response_time": f'{run_time} seconds'}


def pretty_query(phrase, query_result):
    labels = list(query_result["results"].keys())
    values = list(query_result["results"].values())
    comments_count = query_result["comments_count"]
    response_time = query_result["response_time"]
    fig1, ax1 = plt.subplots()
    ax1.pie(values, autopct='%1.1f%%', shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.legend(labels=['%s, %1.1f%%' % (l, s) for l, s in zip(labels, values)])
    plt.title(f'phrase: {phrase} Total comments:{str(comments_count)} response time:{str(response_time)}.')
    bucket_name = os.environ["BUCKET"]
    buffer = BytesIO()
    s3 = boto3.resource('s3')
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    obj = s3.Object(
        bucket_name=bucket_name,
        key=f'{phrase}.png'
    )
    obj.put(Body=buffer, ACL='public-read', ContentType='image/png')
    object_url = "https://%s.s3.amazonaws.com/%s" % (bucket_name, f'{phrase}.png')
    print(f'objecutrl: {object_url}')
    return {"statusCode": 302, "headers": {"location": object_url}}


def query(event, context):
    query_string_parameters = event["pathParameters"]
    phrase = query_string_parameters["phrase"]
    print(query_string_parameters)
    query_result = run(phrase)
    comments_count = query_result["comments_count"]
    if comments_count == 0:
        return {"statusCode": 416}

    if "pretty" in query_string_parameters:
        if query_string_parameters["pretty"] == "pretty":
            return pretty_query(phrase, query_result)
        else:
            return {"statusCode": 416}

    return {"statusCode": 200, "body": json.dumps(query_result)}

# hello(0,0)
