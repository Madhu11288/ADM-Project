from flask import Flask, Response, render_template
from rediscluster import RedisCluster
import time

app = Flask(__name__)
startup_nodes = [{"host": "10.0.0.30", "port": "7000"}]
redis = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)


def trending_tweets():
    trending_hashtags_data = redis.zrange("trending-topics", -20, -1)
    trending_hashtags = []
    for hashtag in trending_hashtags_data:
        trending_hashtags.append(hashtag.split(":")[1])
    print(trending_hashtags)
    yield 'trending_tweets: %s\n\n' % trending_hashtags


def tweets_sliding_window():
    current_time_milliseconds = time.time() * 1000
    five_mins_past = 0.5 * 60 * 1000
    time_five_mins_ago_milliseconds = current_time_milliseconds - five_mins_past
    tweet_ids = redis.zrangebyscore("tweet-time-series", int(time_five_mins_ago_milliseconds),
                                 int(current_time_milliseconds))

    tweets = redis.mget(tweet_ids)
    print(tweets)
    yield 'tweets: %s\n\n' % tweets


@app.route('/')
def show_homepage():
    return render_template("index.html")


@app.route('/trending-hashtags-stream')
def hashtags_stream():
    return Response(trending_tweets(), mimetype="text/event-stream")


@app.route('/tweets-stream')
def tweets_stream():
    return Response(tweets_sliding_window(), mimetype="text/event-stream")


if __name__ == '__main__':
    app.run(threaded=True,
    host='0.0.0.0'
)