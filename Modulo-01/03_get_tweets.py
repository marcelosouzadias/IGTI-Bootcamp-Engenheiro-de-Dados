import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime
import requests

#Configuração das variaveis da chave de acesso

consumer_key = "consumer_key"
consumer_secret = "consumer_secret"

access_token = "access_token"
access_token_secret = "access_token_secret"

#Definir arquivo de saida dos Tweets
data_now = datetime.now().strftime("%y-%m-%d-%H-%M-%S")
out = open(f"./twitter/collect_tweets-{data_now}.txt", "w")

# url de inserção dos tweets no elastic
url = "http://localhost:9200/twitter/_doc"

headers = {
  'Content-Type': 'application/json'
}

# Implemantação da clase de conexão com o Twitter

class MyListener(StreamListener):
    def on_data(self, data):
        #print(data)
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        #print(json.dumps(itemString))
        response = requests.request("POST", url, headers=headers, data=data)
        print(response.text)
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth,l)
    stream.filter(track=['#covid','#covid19','COVID','COVID19'])