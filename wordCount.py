#A Spark script in python to display the wordcount of the input file 
#Save this file as wordcount.py and then use the following command to run it
# spark-submit --master local wordcount.py to run in local mode or
# spark-submit --master yarn wordcount.py to run in yarn mode or


from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Wordcount")
sc = SparkContext(conf=conf)

file = sc.textFile("/user/cloudera/wordcount.txt")
fileFlat = file.flatMap(lambda x: x.split(", "))
fileMap = fileFlat.map(lambda x: (x,1))
fileReduce = fileMap.reduceByKey(lambda x,y: x+y)
fileReduce.saveAsTextFile("/user/cloudera/outputs/wordcount")
