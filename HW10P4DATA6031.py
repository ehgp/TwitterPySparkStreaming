from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple

sc = SparkContext()

ssc = StreamingContext(sc, 1 )
sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream('X.X.X.X', 5555)
lines = socket_stream.window( 1 )

fields = ("tag", "count" )
Tweet = namedtuple( 'Tweet', fields )
lines.flatMap( lambda text: text.split( " " ) ).filter( lambda word: word.lower().startswith("#") ).map( lambda word: ( word.lower(), 1 ) ).reduceByKey( lambda a, b: a + b ).map( lambda rec: Tweet( rec[0], rec[1] ) ).foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") ).limit(10).createOrReplaceTempView("tweets") )
ssc.start()
result = sqlContext.sql("SELECT tag, count from Tweet")
result.show()

