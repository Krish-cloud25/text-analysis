from pyspark import SparkContext
import re

def clean_text(line):
    # Remove label (e.g., "ham", "spam") and keep the message
    parts = line.split("\t")
    text = parts[1] if len(parts) > 1 else line
    # Remove non-letter characters and lowercase the words
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    return text.lower().split()

sc = SparkContext("local[*]", "CleanWordCount")
rdd = sc.textFile("dataset_unzipped/SMSSpamCollection")
words = rdd.flatMap(clean_text)
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
word_counts.saveAsTextFile("clean_output")

