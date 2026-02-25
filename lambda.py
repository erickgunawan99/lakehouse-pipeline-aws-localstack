import urllib.request
import json
import os

def handler(event, context):
    # 1. Parse the S3 Event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    
    payload = {
        "bucket": bucket,
        "key": key
    }
    
    # 2. Prepare the HTTP request
    # Use the service name (spark-master since flask server is running on spark-master container)
    # defined in docker-compose and flask app port (5000)
    url = "http://spark-master:5000/run"
    data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    
    print(f"Sending job request to Flask for file: s3://{bucket}/{key}")
    
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = response.read().decode('utf-8')
            print(f"Flask Response: {result}")
            return {"status": "triggered", "details": result}
    except Exception as e:
        print(f"Error contacting Flask: {str(e)}")
        raise e