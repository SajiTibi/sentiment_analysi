import json
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import time
from timeit import default_timer as timer
from typing import List

import boto3
# import matplotlib.pyplot as plt
import requests
from v0 import collect as v0_collect


def contains_phrase(phrase: str, headline: str) -> bool:
    return phrase.lower() in headline.lower()


def get_topic_title(topic_id: int) -> (int, str):
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{topic_id}.json"
    data = requests.get(topic_url).json()
    return topic_id, data["title"]


def get_valid_topics(topic_id: str, phrase: str):
    topic_url = f"https://hacker-news.firebaseio.com/v0/item/{topic_id}.json"
    data = requests.get(topic_url).json()
    if contains_phrase(phrase, data["title"]):
        return topic_id
    return None


def get_comments(topic_id):
    #  support nested comments
    topic_url = f"https://hn.algolia.com/api/v1/search?tags=comment,story_{topic_id}"
    try:
        data = requests.get(topic_url).json()
    except:
        print(f'invalid {topic_id} topic.')
        return []
    topic_comments = data["hits"]
    comments = []
    for comment in topic_comments:
        if comment["comment_text"]:
            comments.append(comment["comment_text"])
    return comments


def write_topics_to_db(topics_ids: List[int]):
    with ThreadPoolExecutor()as pool:
        all_topics = [topic_description for topic_description in (list(pool.map(get_topic_title, topics_ids)))]
    n = 25
    topics_ids_chunks = [all_topics[i:i + n] for i in range(0, len(all_topics), n)]
    client = boto3.client(service_name='dynamodb', region_name="us-east-1")
    for ti_chunk in topics_ids_chunks:
        request_keys = [{'PutRequest': {"Item": {"topic": {"N": str(topic[0])}, "title": {"S": topic[1]}}}} for topic in
                        ti_chunk]
        response = client.batch_write_item(RequestItems={"topicsTable": request_keys})
        # print(response)


def get_topics_from_db(topics_ids: List[int]) -> List[str]:
    start = timer()
    n = 50
    topics_ids_chunks = [topics_ids[i:i + n] for i in range(0, len(topics_ids), n)]
    client = boto3.client(service_name='dynamodb', region_name="us-east-1")
    topic_id_titles_map = dict()
    for ti_chunk in topics_ids_chunks:
        request_keys = {'Keys': [{"topic": {"N": str(topic_id)}}for topic_id in ti_chunk]}
        response = client.batch_get_item(RequestItems={"topicsTable": request_keys})
        found_topics = response["Responses"]["topicsTable"]
        time.sleep(500)
        for i in found_topics:
            topic_id_titles_map[i["topic"]["N"]] = i["title"]["S"]
    run_time = timer()-start
    print(len(topic_id_titles_map))
    print(f'getting topics from DB run_time: {run_time}')
def collect(phrase: str) -> List[str]:
    top_topics_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    topic_ids = requests.get(top_topics_url).json()
    x = get_topics_from_db(topic_ids)
    valid_topic_ids = []
    s = timer()
    with ThreadPoolExecutor()as pool:
        for topic_id in (list(pool.map(get_valid_topics, topic_ids, repeat(phrase)))):
            if topic_id:
                valid_topic_ids.append(topic_id)
    e1 = timer() - s
    print(f'took {e1} and got {len(valid_topic_ids)} valid topics ')

    with ThreadPoolExecutor()as pool:
        comments_txt = [comment for comment in (list(pool.map(get_comments, valid_topic_ids))) if
                        comment and len(comment) < 5000]
    e2 = timer() - s - e1
    comments_txt = sum(comments_txt, [])
    print(f' took {e2} and got {len(comments_txt)} comments_txt')
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


def run(phrase: str, version: str) -> dict:
    start = timer()
    if version == "v0":
        comments = v0_collect(phrase)
    else:
        comments = collect(phrase)
    e1 = timer() - start
    print(f'collecting comments: {e1}')
    comments_analysis = get_comments_analysis(comments)
    e2 = timer() - start - e1
    print(f'analysing comments: {e2}')
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


# def pretty_query(phrase: str, query_result: dict) -> dict:
#     labels = list(query_result["results"].keys())
#     values = list(query_result["results"].values())
#     comments_count = query_result["comments_count"]
#     response_time = query_result["response_time"]
#     fig1, ax1 = plt.subplots()
#     ax1.pie(values, shadow=True, startangle=90)
#     ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
#     plt.legend(labels=['%s, %s' % (l, s) for l, s in zip(labels, values)])
#     plt.title(f'phrase: {phrase} Total comments:{str(comments_count)} response time:{str(response_time)}.')
#     bucket_name = os.environ["BUCKET"]
#     buffer = BytesIO()
#     s3 = boto3.resource('s3')
#     plt.savefig(buffer, format='png')
#     buffer.seek(0)
#     obj = s3.Object(
#         bucket_name=bucket_name,
#         key=f'{phrase}.png'
#     )
#     obj.put(Body=buffer, ACL='public-read', ContentType='image/png')
#     object_url = "https://%s.s3.amazonaws.com/%s" % (bucket_name, f'{phrase}.png')
#     print(f'objecutrl: {object_url}')
#     return {"statusCode": 302, "headers": {"location": object_url}}


def query(event, context):
    query_string_parameters = event["pathParameters"]
    phrase = query_string_parameters["phrase"]
    print(query_string_parameters)
    v = query_string_parameters["v"]

    query_result = run(phrase, v)
    comments_count = query_result["comments_count"]
    if comments_count == 0:
        return {"statusCode": 416}

    if "pretty" in query_string_parameters:
        if query_string_parameters["pretty"] == "pretty":
            pass
            # return pretty_query(phrase, query_result)
        else:
            return {"statusCode": 416}

    return {"statusCode": 200, "body": json.dumps(query_result)}


if __name__ == '__main__':
    run("corona", "v1")
