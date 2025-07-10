import time
from collections import Counter

start = time.time()

with open("dataset_unzipped/SMSSpamCollection_large", "r") as file:

    text = file.read().lower()

words = text.split()
word_counts = Counter(words)

# Top 10 words
top_10 = word_counts.most_common(10)
print("==== Top 10 Words (Sequential) ====")
for word, count in top_10:
    print(f"{word}: {count}")

end = time.time()
print(f"\n[Sequential] Completed in {end - start:.2f} seconds")
