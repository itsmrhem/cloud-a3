import json
import boto3
import requests
from requests.auth import HTTPBasicAuth
import os

# OpenSearch configuration
ES_ENDPOINT = os.environ['ES_ENDPOINT']  # ex: https://search-photos-xyz...amazonaws.com
INDEX = "photos"
ES_USER = os.environ['ES_USER']
ES_PASSWORD = os.environ['ES_PASSWORD']

# AWS clients
s3 = boto3.client('s3')
lex = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    # Extract query parameter from API Gateway
    print("event", event)
    print(context)
    query = event.get('queryStringParameters', {}).get('q', '')
    if not query:
        return {"statusCode": 400, "body": json.dumps({"message": "Missing query parameter 'q'"})}

    # Send query to Lex to disambiguate keywords
    lex_response = lex.recognize_text(
        botId=os.environ['LEX_BOT_ID'],
        botAliasId=os.environ['LEX_BOT_ALIAS_ID'],
        localeId='en_US',
        sessionId='search-session',
        text=query
    )
    
    slots = lex_response.get('interpretations', [{}])[0].get('intent', {}).get('slots', {})
    keywords = []
    if 'SearchKeywords' in slots and slots['SearchKeywords'] and slots['SearchKeywords'].get('value'):
        keywords.append(slots['SearchKeywords']['value']['interpretedValue'])

    if not keywords:
        # Return empty array if no keywords found
        print("No keywords found")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"results": []})
        }
    print("keywords", keywords)
    # Query OpenSearch
    query_body = {
        "query": {
            "bool": {
                "should": [{"match": {"labels": kw}} for kw in keywords]
            }
        }
    }
    es_url = f"{ES_ENDPOINT}/{INDEX}/_search"
    es_response = requests.get(es_url, auth=HTTPBasicAuth(ES_USER, ES_PASSWORD), headers={"Content-Type": "application/json"}, json=query_body)
    hits = es_response.json().get('hits', {}).get('hits', [])

    # Prepare results with signed URLs
    results = []
    for hit in hits:
        source = hit['_source']
        bucket = source['bucket']
        key = source['objectKey']
        labels = source.get('labels', [])

        # Generate presigned URL
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600  # 1 hour expiry
        )

        results.append({"url": url, "labels": labels})

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"results": results})
    }
