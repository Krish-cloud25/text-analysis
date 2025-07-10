from pyspark import SparkContext
import time

# Measure execution time
start = time.time()

# Create Spark context
sc = SparkContext("local[*]", "WordCount")
sc.setLogLevel("ERROR")

# Read the large dataset
dataset_path = "dataset_unzipped/SMSSpamCollection_large"
print(f"[INFO] Reading file: {dataset_path}")
text_file = sc.textFile(dataset_path)

# Preview sample
sample = text_file.take(3)
print("[DEBUG] Sample lines:", sample)

# Perform word count
print("[INFO] Performing word count...")
counts = (
    text_file.flatMap(lambda line: line.split())
             .map(lambda word: (word.lower(), 1))
             .reduceByKey(lambda a, b: a + b)
)

# Get top 10 words
print("[INFO] Extracting top 10 words...")
top_10 = counts.takeOrdered(10, key=lambda x: -x[1])

# Print results
print("\n==== Top 10 Words (Spark Parallel) ====")
for word, count in top_10:
    print(f"{word}: {count}")

# Execution time
end = time.time()
print(f"\n[Parallel Spark] Completed in {end - start:.2f} seconds")

# Stop Spark
sc.stop()
