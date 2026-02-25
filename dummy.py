import io
import random
import string
import boto3 
from datetime import date, datetime, timedelta, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from botocore.exceptions import ClientError
# ------------------
# Config
# ------------------
random.seed(42)
NUM_EVENTS = 50_000   # increase if you want bigger
NUM_IPS = 10_000
# 1. Setup LocalStack S3 Client
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",  # Key for LocalStack
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

s3_bucket = "lakehouse-warehouse"

EVENT_TOPICS = ["search", "product", "checkout", "profile"]
ACTIONS = ["view", "click", "submit", "purchase"]
BROWSERS = ["chrome", "firefox", "safari", "edge"]
OS = ["ios", "android", "windows", "macos", "linux"]

COUNTRIES = [
    ("US", "New York", 40.7, -74.0),
    ("DE", "Berlin", 52.5, 13.4),
    ("ID", "Jakarta", -6.2, 106.8),
    ("SG", "Singapore", 1.3, 103.8),
    ("JP", "Tokyo", 35.6, 139.6),
]

time = datetime.now(timezone.utc).strftime("%H%M%S")
# ------------------
# Helpers
# ------------------
def random_ip():
    return ".".join(str(random.randint(1, 255)) for _ in range(4))

def random_string(n=8):
    return "".join(random.choices(string.ascii_lowercase, k=n))

# ------------------
# Generate IP dimension
# ------------------
ips = [random_ip() for _ in range(NUM_IPS)]

geo_rows = []
for ip in ips:
    c = random.choice(COUNTRIES)
    geo_rows.append({
        "ip_address": ip,
        "country": c[0],
        "city": c[1],
        "lat": c[2],
        "lon": c[3],
    })

geo_df = pd.DataFrame(geo_rows)

geo_buffer = io.BytesIO()
geo_df.to_parquet(geo_buffer, engine='pyarrow', index=False)

try:
    s3_client.put_object(
        Bucket=s3_bucket,
        Key="data/ip_geolocation.parquet",
        Body=geo_buffer.getvalue(),
        IfNoneMatch="*"  # Only upload if the object doesn't already exist since IP geolocation data is static and can be reused across multiple runs
    )
    print("✅ Uploaded new geo data.")
except ClientError as e:
    # Check if the error is specifically "Precondition Failed" (412)ss
    if e.response['Error']['Code'] == "PreconditionFailed":
        print("⏭️ Geo data already exists. Skipping upload.")
    else:
        raise

# Generate user events
start_time = datetime.now(timezone.utc) - timedelta(days=7)


event_rows = []
for i in range(NUM_EVENTS):
    ts = start_time + timedelta(seconds=random.randint(0, 7 * 24 * 3600))

    event_rows.append({
        "event_id": "event_" + str(i) + "_" + random_string(8),
        "user_id": "user_" + str(i) + "_" + str(random.randint(0, 10000)) + "_" + random_string(5),
        "event_time": ts,
        "topic": random.choice(EVENT_TOPICS),
        "action": random.choice(ACTIONS),
        "ip_address": random.choice(ips),
        "device": {
            "os": random.choice(OS),
            "browser": random.choice(BROWSERS),
        },
        "page": {
            "url": f"/{random_string(5)}",
            "referrer": f"/{random_string(5)}",
        }
    })

events_df = pd.DataFrame(event_rows)

events_table = pa.Table.from_pandas(events_df, preserve_index=False)
events_buffer = io.BytesIO()

# Write the Parquet data to the in-memory buffer
pq.write_table(events_table, events_buffer, compression="snappy")

# Upload the bytes from the buffer to LocalStack
s3_client.put_object(
    Bucket=s3_bucket,
    Key=f"data/user_events_{time}.parquet",
    Body=events_buffer.getvalue() )

# print list of generated files in S3
print("Generated Parquet files:")
for obj in s3_client.list_objects_v2(Bucket=s3_bucket, Prefix="data/")["Contents"]:
    print(f"s3://{s3_bucket}/{obj['Key']}")
