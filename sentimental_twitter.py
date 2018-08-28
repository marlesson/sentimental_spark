# Coleta e análise de sentimento no twitter
#
# https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter

from __future__ import print_function
import csv
import json

#https://github.com/dpkp/kafka-python
from kafka import KafkaProducer

# https://github.com/geduldig/TwitterAPI
from TwitterAPI import TwitterAPI 

#https://textblob.readthedocs.io/en/dev/
from textblob import TextBlob 

# Termos de busca
TRACK_TERM = 'brasil'

# Twitter Conf
consumer_key        = 'ZsA3TtsNVA6aFiaXzw78JLrfw'
consumer_secret     = 'ryD7OeU9CEbxbh9LQeOBrGltkvmrmMrVGvQXF7HLVrhWu8elEV'
access_token_key    = '38406043-WQ9p5NzDZhCGoEO8j95itZocDshJ7J6t1bvsuQdQA'
access_token_secret = 'yYZbV4uAytZi2WLVotTzeto8liLV21lZokXieAYuQPiAF'

# Kafka Conf
k_topic  = 'tweets'
k_server = 'localhost:9092'

def main():
  # API Access
  api   = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
  kafka = KafkaProducer(bootstrap_servers=k_server) 

  # Open Stream
  result = api.request('statuses/filter', {'track': TRACK_TERM})  
  for tweet in result:
    if filter_tweet(tweet):
      send_kafka(kafka, tweet)

# -POLARITY - é um valor contínuo que varia de -1.0 a 1.0, 
# sendo -1.0 referente a 100% negativo e 1.0 a 100% positivo.
def polarity(text):
  try:
    frase    = TextBlob(text)
    traducao = TextBlob(str(frase.translate(to='en')))
    polarity = traducao.sentiment.polarity
  except: 
    polarity = 0

  return polarity

# Filtra o tweet apenas em Português
def filter_tweet(tweet):
  if 'text' in tweet:
    if tweet['lang'] == 'pt':
      return True
  else:
    return False

# Salva o Tweet com a Polaridade no arquivo tweets.csv
def send_kafka(kafka, tweet):
  id_str, text, _polarity = tweet["id"], tweet["text"], polarity(tweet['text'])

  print("{} - {} => {}".format(id_str, text, _polarity))
  kafka.send(k_topic, json.dumps({'text': text, 'polarity': _polarity}).encode('utf-8'))

if __name__ == "__main__":
  main()