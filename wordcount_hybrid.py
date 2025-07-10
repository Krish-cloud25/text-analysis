import multiprocessing
import time
from collections import Counter
import boto3

# Simulated complex word processing logic
def process_chunk(chunk):
    time.sleep(1)  # Simulate EC2 delay
    words = chunk.lower().split()
    words = [word + "_extra" for word in words]  # Simulate processing
    return Counter(words)

if __name__ == "__main__":
    start = time.time()

    print("[INFO] Reading large dataset...")
    with open("dataset_unzipped/SMSSpamCollection_large", "r") as f:
        lines = f.readlines()

    print("[INFO] Splitting data for multiprocessing...")
    num_chunks = 4  # Simulate 4 EC2 instances
    chunk_size = len(lines) // num_chunks
    chunks = [''.join(lines[i*chunk_size:(i+1)*chunk_size]) for i in range(num_chunks)]

    print("[INFO] Starting multiprocessing pool...")
    with multiprocessing.Pool(num_chunks) as pool:
        counters = pool.map(process_chunk, chunks)

    print("[INFO] Aggregating results...")
    total_counter = Counter()
    for counter in counters:
        total_counter.update(counter)

    top_10 = total_counter.most_common(10)
    print("\n==== Top 10 Words (Hybrid Parallelism) ====")
    for word, count in top_10:
        print(f"{word}: {count}")

    end = time.time()
    exec_time = end - start
    throughput = len(lines) / exec_time if exec_time > 0 else 0

    print(f"\n[Hybrid Parallel] Completed in {exec_time:.2f} seconds")
    print(f"[INFO] Throughput: {throughput:.2f} records/second")

    # ===== Push to CloudWatch =====
    print("[INFO] Pushing metrics to CloudWatch...")
    cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')  # Adjust if needed

    cloudwatch.put_metric_data(
        Namespace='MyApp/Performance',
        MetricData=[
            {
                'MetricName': 'ExecutionTime',
                'Value': exec_time,
                'Unit': 'Seconds'
            },
            {
                'MetricName': 'Throughput',
                'Value': throughput,
                'Unit': 'Count/Second'
            }
        ]
    )
    print("✅ Metrics pushed to CloudWatch.")
