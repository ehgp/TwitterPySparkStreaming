"""Write a Spark Stream program to stream and read data from 
Twitter and then display recent tweets related to hashtag #umbc.
Hints: 
I.	Spark has a package called Twitter.utils. This package 
contains all the functions to stream data from Twitter.
II.	Java, Scala, or Python are all accepted.
III.	Results visualization is important, so think about 
ways to make the results clear.
"""

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple

consumer_key    = 'X'
consumer_secret = 'X'
access_token    = 'X'
access_secret   = 'X'

# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads( data )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True
  
def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['#COVID-19'])

host = 'X.X.X.X'
port = 5555
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((host,port))
s.listen(1)
c, addr = s.accept()
print("Received request from: " + str(addr))
print(c)
sendData(c)


