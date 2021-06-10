from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pymongo
import pandas as pd

import config
from time import sleep
import json

# CRIANDO A AUTENTICAÇÃO
auth = OAuthHandler(config.API_Key, config.API_Secret_Key)
auth.set_access_token(config.Access_Token, config.Access_Token_Secret)


# CLASS PARA CAPTURAR OS DADOS
class MyListener(StreamListener):
    def on_data(self, raw_data):
        tweet = json.loads(raw_data)
        created_at = tweet['created_at']
        id_str = tweet['id_str']
        text = tweet['text']
        obj = {'created_at': created_at,
               'id_str': id_str,
               'text': text}
        tweetind = col.insert_one(obj).inserted_id
        print(obj)
        return True


mylistener = MyListener()

mystream = Stream(auth, listener=mylistener)

client = pymongo.MongoClient()

db = client.twitterdb
col = db.tweets

# VARIAL COM AS PALAVRAS CHAVES DE BUSCA NO TWITTER
keywords = ['Big Data', 'Python', 'Data Mining', 'Data Science']

# COLETANDO OS DADOS
# mystream.filter(track=keywords, encoding='UTF-8')
# sleep(30)
# mystream.disconnect()


# CRIANDO DATASET COM O RETORNO DO MONGO
dataset = [{'created_at': item['created_at'], 'text': item['text'], } for item in col.find()]
df = pd.DataFrame(dataset)

#print(df)

cv = CountVectorizer()
count_matrix = cv.fit_transform(df.text)

# CONTANDO O NÚMERO DE OCORRÊNCIAS DAS PRICIPAIS PALAVRAS DO DATASET
word_count = pd.DataFrame(cv.get_feature_names(), columns=['Word'])
word_count['count'] = count_matrix.sum(axis=0).tolist()[0]
word_count = word_count.sort_values('count', ascending=False).reset_index(drop=True)
print(word_count[:50])