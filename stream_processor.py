from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create SparkContext and StreamingContext
sc = SparkContext("local[2]", "StreamingApp")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)  # 2s batch

# Checkpoint is required for reduceByKeyAndWindow
ssc.checkpoint("checkpoint")

# Create DStream from socket
lines = ssc.socketTextStream("localhost", 9999)

# Process the stream
words = lines.flatMap(lambda line: line.strip().split())
pairs = words.map(lambda word: (word.lower(), 1))

# Reduce over window: 10s window, 2s slide
windowed_counts = pairs.reduceByKeyAndWindow(
    lambda x, y: x + y,
    lambda x, y: x - y,
    10,
    2
)

def print_top_rdd(rdd):
    sorted_items = rdd.sortBy(lambda x: -x[1])
    top5 = sorted_items.take(5)
    if top5:
        print("\n==== Top 5 Words ====")
        for word, count in top5:
            print(f"{word}: {count}")

windowed_counts.foreachRDD(print_top_rdd)

ssc.start()
ssc.awaitTermination()
