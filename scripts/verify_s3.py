import boto3
import os
from dotenv import load_dotenv

load_dotenv(".env")

S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def list_bucket_contents():
    print(f"Connecting to {S3_ENDPOINT_URL} bucket {S3_BUCKET_NAME}...")
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            endpoint_url=S3_ENDPOINT_URL,
        )
        
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        
        if 'Contents' in response:
            print(f"Found {len(response['Contents'])} objects:")
            total_size = 0
            for obj in response['Contents']:
                print(f"- {obj['Key']} ({obj['Size']} bytes)")
                total_size += obj['Size']
            print(f"Total size visible via API: {total_size / 1024 / 1024:.2f} MB")
        else:
            print("Bucket is empty (or no objects returned).")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_bucket_contents()
