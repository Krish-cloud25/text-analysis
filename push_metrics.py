import boto3
from datetime import datetime

# Create a CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')  # Change region if needed

# Sample performance metrics
execution_time = 0.10  # seconds
throughput = 4779      # records per second

# Current UTC timestamp for accurate plotting
timestamp = datetime.utcnow()

# Push metrics to CloudWatch under custom namespace
response = cloudwatch.put_metric_data(
    Namespace='MyApp/Performance',  # Custom namespace
    MetricData=[
        {
            'MetricName': 'ExecutionTime',
            'Dimensions': [
                {
                    'Name': 'InstanceType',
                    'Value': 'EC2-Benchmark'
                }
            ],
            'Timestamp': timestamp,
            'Value': execution_time,
            'Unit': 'Seconds'
        },
        {
            'MetricName': 'Throughput',
            'Dimensions': [
                {
                    'Name': 'InstanceType',
                    'Value': 'EC2-Benchmark'
                }
            ],
            'Timestamp': timestamp,
            'Value': throughput,
            'Unit': 'Count/Second'
        }
    ]
)

print("✅ Metrics pushed to CloudWatch successfully.")
