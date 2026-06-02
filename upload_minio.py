import boto3
import glob
import os

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

for f in glob.glob("data/labels/*.json"):
    s3.upload_file(f, "labels-raw", os.path.basename(f))
    print("Uploadé:", f)