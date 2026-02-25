from flask import Flask, request
import subprocess

app = Flask(__name__)

# Endpoint to receive job requests from Lambda
@app.route("/run", methods=["POST"])
def run():
    data = request.json
    bucket = data["bucket"]
    key = data["key"]
    print(f"Received job request for file: s3://{bucket}/{key}")

    try:
        result = subprocess.Popen([
            "spark-submit",
            "/opt/spark/jobs/spark_processor.py",
            bucket,
            key
        ])
        print(f"Spark job submitted with PID: {result.pid}")
    except Exception as e:
        print(f"Error submitting Spark job: {e}")
        raise e

    return {"status": "spark submitted"}
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
