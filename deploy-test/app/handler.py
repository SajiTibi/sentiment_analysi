import json
import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from timeit import default_timer as timer
from typing import List

import boto3
import bs4
from . import config
# import config
import matplotlib.pyplot as plt
import pymongo
import requests


def contains_phrase(phrase: str, headline: str) -> bool:
    phrase = phrase.replace('_', ' ')
    return phrase.lower() in headline.lower()


def get_topic_title(topic_id: int) -> (int, str):
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{topic_id}.json"
    data = requests.get(topic_url).json()
    return topic_id, data["title"]


def get_kids(topic_id: str) -> List[str]:
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{topic_id}.json"
    data = requests.get(topic_url).json()
    return data["kids"] if "kids" in data else []


def get_comment(comment_id: str) -> str:
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{comment_id}.json"
    data = requests.get(topic_url).json()
    if "text" not in data:
        return ""
    return bs4.BeautifulSoup(data["text"], features="html.parser").get_text()


def get_topic_comments(topic_id: int) -> List[str]:
    #  support nested comments
    topic_url = f"https://hn.algolia.com/api/v1/search?tags=comment,story_{topic_id}"
    data = requests.get(topic_url).json()
    topic_comments = data["hits"]
    comments_lst = []
    for comment in topic_comments:
        if comment["comment_text"]:
            plain_text = bs4.BeautifulSoup(comment["comment_text"], features="html.parser").get_text()
            comments_lst.append(plain_text)
    return comments_lst


def get_valid_topic_ids(phrase: str, topics_db: pymongo.collection.Collection) -> List[int]:
    top_topics_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    topic_ids = requests.get(top_topics_url).json()
    topics_mapping = get_topics_from_db(topic_ids, topics_db)
    return [topic_id for topic_id in topics_mapping if contains_phrase(phrase, topics_mapping[topic_id])]


def get_comments_analysis(comments: List[str]) -> List[dict]:
    # the data is partitioned to chunks of 25 as its the max amount can be processed by batch_detect_sentiment.
    n = 25
    comprehend = boto3.client(service_name='comprehend')
    comments_chunks = [comments[i:i + n] for i in range(0, len(comments), n)]
    comments_analysis = []

    for comments_chunk in comments_chunks:
        comments_analysis.append(json.loads(json.dumps(comprehend.batch_detect_sentiment
                                                       (TextList=comments_chunk, LanguageCode='en'), sort_keys=True,
                                                       indent=4))["ResultList"])
    return sum(comments_analysis, [])


def write_topics_to_db(topics_ids: List[int], topics_db: pymongo.collection.Collection) -> dict:
    if not topics_ids:
        return {}
    with ThreadPoolExecutor()as pool:
        all_topics = [topic_description for topic_description in (list(pool.map(get_topic_title, topics_ids)))]
    topics_docs = [{"topic_id": topic_description[0], "topic_title": topic_description[1]} for topic_description in
                   all_topics]
    topics_db.insert_many(topics_docs)  # todo more fail checks
    topics_mapping = {topic[0]: topic[1] for topic in all_topics}
    return topics_mapping


def get_topics_from_db(topics_ids: List[int], topics_db: pymongo.collection.Collection) -> dict:
    topics_ids_mapping = dict()
    topics_query = {"topic_id": {"$in": topics_ids}}
    query_result = topics_db.find(topics_query)
    for topic_item in query_result:
        topics_ids_mapping[topic_item["topic_id"]] = topic_item["topic_title"]
    new_topics = [topic_id for topic_id in topics_ids if topic_id not in topics_ids_mapping]
    new_topics = write_topics_to_db(new_topics, topics_db)
    topics_ids_mapping.update(new_topics)
    return topics_ids_mapping


def get_comments_v1(phrase: str, topics_db: pymongo.collection.Collection) -> List[str]:
    valid_topic_ids = get_valid_topic_ids(phrase, topics_db)
    with ThreadPoolExecutor()as pool:
        comments_txt = []
        for topic_comments in list(pool.map(get_topic_comments, valid_topic_ids)):
            topic_comments = [comment for comment in topic_comments if len(comment) < 5000]
            comments_txt.append(topic_comments)
    comments_txt = sum(comments_txt, [])
    return comments_txt


def get_comments_v0(phrase: str, topics_db: pymongo.collection.Collection) -> List[str]:
    valid_topic_ids = get_valid_topic_ids(phrase, topics_db)
    with ThreadPoolExecutor()as pool:
        comments_ids = [comment_ids for comment_ids in (list(pool.map(get_kids, valid_topic_ids)))]
    comments_ids = sum(comments_ids, [])
    with ThreadPoolExecutor()as pool:
        comments_txt = [comment for comment in (list(pool.map(get_comment, comments_ids))) if
                        comment and len(comment) < 5000]
    return comments_txt


def run(phrase: str, version: str, topics_db: pymongo.collection.Collection) -> dict:
    start = timer()
    if version == "v0":
        comments = get_comments_v0(phrase, topics_db)
    else:
        comments = get_comments_v1(phrase, topics_db)
    comments_analysis = get_comments_analysis(comments)
    counter_d = {}
    all_count = 0
    for comment in comments_analysis:
        comment_sentiment = comment["Sentiment"]
        if comment_sentiment in counter_d:
            counter_d[comment_sentiment] += 1
        else:
            counter_d[comment_sentiment] = 1
        all_count += 1
    for k in counter_d:
        counter_d[k] = "%.2f" % ((counter_d[k] / all_count) * 100)
    end = timer()
    run_time = "%.2f" % (end - start)
    return {"results": counter_d, "comments_count": all_count, "response_time": f'{run_time} seconds'}


def pretty_query(phrase: str, query_result: dict) -> dict:
    labels = list(query_result["results"].keys())
    values = list(query_result["results"].values())
    comments_count = query_result["comments_count"]
    response_time = query_result["response_time"]
    fig1, ax1 = plt.subplots()
    ax1.pie(values, shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.legend(labels=['%s, %s %%' % (l, s) for l, s in zip(labels, values)])
    plt.title(f'phrase: {phrase} Total comments:{str(comments_count)} response time:{str(response_time)}.')
    bucket_name = os.environ["BUCKET"]
    buffer = BytesIO()
    s3 = boto3.resource('s3')
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    # todo: add to the filename a timestamp or other flag to avoid collisions
    obj = s3.Object(bucket_name=bucket_name, key=f'{phrase}.png')
    obj.put(Body=buffer, ACL='public-read', ContentType='image/png')
    object_url = "https://%s.s3.amazonaws.com/%s" % (bucket_name, f'{phrase}.png')
    return {"statusCode": 302, "headers": {"location": object_url}}


def query(event, context):
    query_string_parameters = event["pathParameters"]
    phrase = query_string_parameters["phrase"]
    print(query_string_parameters)
    v = query_string_parameters["v"]
    client = pymongo.MongoClient(
        f"mongodb+srv://{config.username}:{config.password}@cluster0-lftvo.mongodb.net/test?retryWrites=true&w=majority")
    db = client["topics"]
    db_collection = db["topics_mapping"]
    query_result = run(phrase, v, db_collection)
    comments_count = query_result["comments_count"]
    if comments_count == 0:
        return {"statusCode": 416}

    if "pretty" in query_string_parameters:
        if query_string_parameters["pretty"] == "pretty":
            return pretty_query(phrase, query_result)
        else:
            return {"statusCode": 416}
    return {"statusCode": 200, "body": json.dumps(query_result)}

