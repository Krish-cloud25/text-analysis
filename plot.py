import matplotlib.pyplot as plt

# Sample execution times (in seconds)
labels = ['Sequential', 'Spark Streaming', 'Hybrid Parallel']
execution_times = [0.03, 0.25, 0.10]
throughputs = [16000, 1911, 4779]
latencies = [1.8, 52.3, 12.5]  # in ms

# 1. Execution Time Bar Chart
plt.figure()
plt.bar(labels, execution_times, color='skyblue')
plt.title('Execution Time Comparison')
plt.ylabel('Time (seconds)')
plt.savefig('execution_time.png')

# 2. Throughput Bar Chart
plt.figure()
plt.bar(labels, throughputs, color='lightgreen')
plt.title('Throughput Comparison')
plt.ylabel('Lines per second')
plt.savefig('throughput.png')

# 3. Latency Bar Chart
plt.figure()
plt.bar(labels, latencies, color='salmon')
plt.title('Latency Comparison')
plt.ylabel('Latency (ms)')
plt.savefig('latency.png')

print("✅ Graphs saved as PNG files.")
