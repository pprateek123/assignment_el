import json
import pandas as pd
import urllib3
import boto3


def lambda_handler(event, context):

    http = urllib3.PoolManager()

    url = "https://api.artic.edu/api/v1/artworks/search"

    response = http.request("GET", url)
    response1 = response.data.decode("utf-8")

    s3 = boto3.client("s3")
    s3.put_object(
        Body=response.data,
        Bucket="apprentice-training-data-prateek-raw",
        Key="raw_data.json",
    )

    json_data = json.loads(response1)
    df = pd.DataFrame(json_data["data"])
    df = df.drop("thumbnail", axis=1)
    new_column_names = {"_score": "scores", "timstamp": "time"}
    df.rename(columns=new_column_names, inplace=True)

    cleaned_json = df.to_json(orient="records")
    s3.put_object(
        Body=cleaned_json,
        Bucket="appretince-training-prateek-data-clean",
        Key="cleaned.json",
    )

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
