from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
es = Elasticsearch()

def get_sentiment(tweet_json):
    tweet_text = tweet_json['text']
    result = TextBlob(tweet_text)

    sentiment_polarity = result.sentiment.polarity

    if(sentiment_polarity > 0):
        tweet_json['sentiment'] = 'positive'
        return(tweet_json)
    elif(sentiment_polarity == 0):
        tweet_json['sentiment'] = 'neutral'
        return(tweet_json)
    else:
        tweet_json['sentiment'] = 'negative'
        return(tweet_json)

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:

        dict_data = json.loads(msg.value)
        
        dict_data = get_sentiment(dict_data)

        tweet = TextBlob(dict_data["text"])
        print(tweet)
        # add text and sentiment info to elasticsearch
        es.index(index="tweet",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "sentiment": dict_data["sentiment"]})
        print('\n')

if __name__ == "__main__":
    main()
