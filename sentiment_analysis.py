from pyspark import SparkContext
from textblob import TextBlob

# Function to classify sentiment
def get_sentiment(line):
    parts = line.split("\t")
    if len(parts) < 2:
        return ("neutral", 1)  # fallback for malformed lines
    text = parts[1]
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return ("positive", 1)
    elif polarity < 0:
        return ("negative", 1)
    else:
        return ("neutral", 1)

# Set up Spark context
sc = SparkContext("local[*]", "SentimentAnalysis")

# Read the dataset
rdd = sc.textFile("dataset_unzipped/SMSSpamCollection")

# Apply sentiment analysis and count
sentiment_counts = rdd.map(get_sentiment).reduceByKey(lambda a, b: a + b)

# Save results
sentiment_counts.saveAsTextFile("sentiment_output")
