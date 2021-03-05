import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime


#Configuração das variaveis da chave de acesso

consumer_key = "v6OAOrL5FmNu369Fym6TRrEwr"
consumer_secret = "7JkMctS3QDk02q14J35fp9JjYJ43AP7FLHnHhmTdfmqE88OaL3"

access_token = "60646343-7Ib39Qw2oOGftjCzXq82FKir0GJWPMrRmHFcv9VBc"
access_token_secret = "eswiXs3SWvCZNtNyc4pw1e7OqPiSEeYYh6iitzitrv3L9"

#Definir arquivo de saida dos Tweets
data_now = datetime.now().strftime("%y-%m-%d-%H-%M-%S")
out = open(f"./twitter/collect_tweets-{data_now}.txt", "w")

# Implemantação da clase de conexão com o Twitter

class MyListener(StreamListener):
    def on_data(self, data):
        print(data)
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth,l)
    stream.filter(track=['#covid','#covid19','COVID','COVID19'])