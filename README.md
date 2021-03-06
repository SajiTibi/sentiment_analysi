# sentiment_analysis
**Brief**
This service allows the users to analyse the comments of the topstories in hacker-news website with given phrase, there is an option for analyzing nested comments(comment on a comment) as well, the analyses done using AWS comprehend services, the results can be retrieved as json or graph . 

**Usage**
`https://vhkgzpk2d7.execute-api.us-east-1.amazonaws.com/dev/{v}/{phrase}/{pretty}`
- {v} 
v0: doesnt support nested comments can be slower for topics with high amount of  replies.
v1: supports nested comments.
- {phrase}: search phrase, use _ as whitespace to seperate words
- {pretty}: optional, adding pretty will display the data as graph.

**Examples:**

- https://vhkgzpk2d7.execute-api.us-east-1.amazonaws.com/dev/v1/corona
>{"results": {"NEUTRAL": "38.35", "NEGATIVE": "42.11", "MIXED": "3.01", "POSITIVE": "16.54"}, "comments_count": 133, "processing_time": "4.99 seconds"}

- https://vhkgzpk2d7.execute-api.us-east-1.amazonaws.com/dev/v1/how_to

> {"results": {"NEUTRAL": "42.86", "NEGATIVE": "38.10", "MIXED": "2.38", "POSITIVE": "16.67"}, "comments_count": 42, "processing_time": "1.34 seconds"}

- https://vhkgzpk2d7.execute-api.us-east-1.amazonaws.com/dev/v1/trump/pretty
![trump](https://user-images.githubusercontent.com/34559152/79369189-3a96ce00-7f59-11ea-98a0-f2c2e0704ea7.png)

- https://vhkgzpk2d7.execute-api.us-east-1.amazonaws.com/dev/v1/corona/anyotherkeyword
   will cause an `HTTP ERROR 416
`


**Challenges:**
- Speed:
 collecting 500 titles each call takes a long time so i tried storing the topic titles in database, first approach was to use dynamodb which is AWS service as well (branch v1). however, due to their policy the read/write requests in provisioned version are super low which makes it impossible to requests 500 topic or write 500 topic or even less. Auto scaling is expensive, so i moved from dynamodb to mongodb which allows read/writing without extra costs.
Making the requests to get each topic title or each comment will also effect the runtime and the bottle neck in this case is the requests number and not the speed connection, so from early stages i used `ThreadPoolExecutor`[1] which allows parallel execution
- API:
The official API provides only ids of comments on the given story, this leads to collecting each comment from each id, however i found another API [2] which provides the all the comments as text on the spot so its possible through only call to get all the comments on story,including nested comments, this is require keyword v1 and the former is v0.
- More available in issues .


**References:**

[1] https://stackoverflow.com/questions/40391898/send-simultaneous-requests-python-all-at-once

[2] https://hn.algolia.com/api
