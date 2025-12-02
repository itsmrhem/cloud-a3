import json
import boto3
import os
import urllib.parse
import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth

# --- ENV VARS ---
ES_ENDPOINT = "https://search-photos-faixytpsvphrppe4zlxax6tpua.us-east-1.es.amazonaws.com"
ES_USERNAME = "admin"
ES_PASSWORD = "Hemanth@123"
INDEX = "photos"
#test
# AWS clients
s3 = boto3.client("s3")
rekognition = boto3.client("rekognition")


def lambda_handler(event, context):

    print("Received event:", json.dumps(event))

    # --------------------------------------------------------
    # 1. Parse S3 Event
    # --------------------------------------------------------
    s3_info = event["Records"][0]["s3"]
    bucket = s3_info["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(s3_info["object"]["key"])   # important fix!
    print("Bucket:", bucket)
    print("Key:", object_key)

    # --------------------------------------------------------
    # 2. Detect labels with Rekognition
    # --------------------------------------------------------
    try:
        rekog_response = rekognition.detect_labels(
            Image={"S3Object": {"Bucket": bucket, "Name": object_key}},
            MaxLabels=10,
            MinConfidence=75
        )
    except Exception as e:
        print("Rekognition error:", str(e))
        raise

    rekog_labels = [label["Name"] for label in rekog_response.get("Labels", [])]
    print("Rekognition labels:", rekog_labels)

    # --------------------------------------------------------
    # 3. Read S3 metadata (x-amz-meta-customLabels)
    # --------------------------------------------------------
    head = s3.head_object(Bucket=bucket, Key=object_key)
    metadata = head.get("Metadata", {})

    raw_custom = metadata.get("customlabels", "")
    custom_labels = (
        [lbl.strip() for lbl in raw_custom.split(",") if lbl.strip()]
        if raw_custom else []
    )

    print("Custom labels:", custom_labels)

    # --------------------------------------------------------
    # 4. Build final label array
    # --------------------------------------------------------
    all_labels = list(set(custom_labels + rekog_labels))

    # --------------------------------------------------------
    # 5. Build document for OpenSearch
    # --------------------------------------------------------
    doc = {
        "objectKey": object_key,
        "bucket": bucket,
        "createdTimestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "labels": all_labels
    }

    print("Document:", json.dumps(doc, indent=2))

    # --------------------------------------------------------
    # 6. Index into Elasticsearch / OpenSearch
    # --------------------------------------------------------
    url = f"{ES_ENDPOINT}/{INDEX}/_doc"

    try:
        response = requests.post(
            url,
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            headers={"Content-Type": "application/json"},
            data=json.dumps(doc)
        )
        print("ES Response:", response.text)
    except Exception as e:
        print("Error indexing into ES:", str(e))
        raise

    return {
        "statusCode": 200,
        "body": json.dumps("Indexing completed")
    }
