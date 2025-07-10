import matplotlib.pyplot as plt

# Load sentiment output from Spark job
data = {}
with open("sentiment_output/part-00000", "r") as f:
    for line in f:
        line = line.strip().replace("(", "").replace(")", "").replace("'", "")
        label, count = line.split(", ")
        data[label] = int(count)

# Plotting
labels = list(data.keys())
values = list(data.values())

plt.figure(figsize=(6, 4))
plt.bar(labels, values, color=["green", "gray", "red"])
plt.xlabel("Sentiment")
plt.ylabel("Count")
plt.title("Sentiment Analysis of SMS Messages")
plt.tight_layout()
plt.savefig("sentiment_bar_chart.png")
print("Chart saved as sentiment_bar_chart.png")
