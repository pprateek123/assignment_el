import json
import psycopg2
import boto3
import pandas as pd
import os
from io import StringIO


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client("s3")
    response = s3.get_object(
        Bucket="appretince-training-prateek-data-clean", Key="cleaned.csv"
    )
    object_content = response["Body"].read()

    csv_obj = object_content.decode("utf-8")
    csv_content = StringIO(csv_obj)

    df = pd.read_csv(csv_content)

    connection = psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    cursor = connection.cursor()

    insert_query = """
    INSERT INTO etl_prateek_movies_table (prim_id ,scores, api_model, is_boosted, api_link, id, title, timestamp)
    VALUES (%s,%s, %s, %s, %s, %s, %s, %s);
    """

    # Iterate through DataFrame rows and insert into the table
    for index, row in df.iterrows():
        values = tuple(row)
        cursor.execute(insert_query, values)

    update_query = """
    UPDATE etl_prateek_movies_table
    SET is_boosted = TRIM(LEADING ' ' FROM is_boosted);
    """

    update_query_round = """
    UPDATE etl_prateek_movies_table
    SET scores = cast(scores as numeric(10,2));
    """
    cursor.execute(update_query_round)

    connection.commit()

    connection.close()

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
