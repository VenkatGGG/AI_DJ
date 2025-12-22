import os
import time
import argparse
import logging
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from tqdm import tqdm
from pathlib import Path

# Add backend to path to import models
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../backend'))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Track, Base

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(".env")
load_dotenv(".env")
# Debug: Print which keys are present (without values)
required_keys = ["S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "DATABASE_URL"]
missing_keys = [k for k in required_keys if not os.getenv(k)]
if missing_keys:
    print(f"CRITICAL WARNING: Missing environment variables: {missing_keys}", flush=True)
else:
    print("All required environment variables found.", flush=True)

# S3 / Railway Object Store Configuration
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "us-east-1") # Default region if not specified

# DB Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION_NAME
    )

def get_db_session():
    if not DATABASE_URL:
        return None
        
    # SQLAlchemy 1.4+ requires postgresql:// scheme
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()

def download_file(url, local_path, retries=3):
    """Download a file from a URL to a local path with retries."""
    for attempt in range(retries):
        try:
            with requests.get(url, stream=True, timeout=15) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2 * (attempt + 1))
    return False

def upload_to_s3(s3_client, local_path, s3_key):
    """Upload a file to S3."""
    try:
        s3_client.upload_file(str(local_path), S3_BUCKET_NAME, s3_key)
        # Using a public read assumption or presuming the bucket policy handles it.
        # If needed, we can generate a URL. For s3 compatible, usually:
        # endpoint/bucket/key
        url = f"{S3_ENDPOINT_URL}/{S3_BUCKET_NAME}/{s3_key}"
        return url
    except NoCredentialsError:
        logger.error("Credentials not available")
        return None
    except Exception as e:
        logger.error(f"Failed to upload {s3_key}: {e}")
        return None

def process_dataset(tsv_path: str, output_dir: str, limit: int = None):
    """
    Process the MTG-Jamendo dataset TSV.
    Expected columns: TRACK_ID, ARTIST_ID, ALBUM_ID, PATH, DURATION, TAGS
    """
    try:
        # quoting=3 is CSV.QUOTE_NONE, helps avoiding parsing errors on some tags
        df = pd.read_csv(tsv_path, sep='\t', on_bad_lines='skip', quoting=3)
    except Exception as e:
        logger.error(f"Failed to read TSV: {e}")
        return
    
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    s3_client = get_s3_client()
    db_session = get_db_session()
    
    # Create bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
    except Exception:
        pass 

    processed_count = 0
    
    for index, row in df.iterrows():
        if limit and processed_count >= limit:
            break
            
        track_id = str(row.get('TRACK_ID', row.get('track_id', row.get('id', ''))))
        
        # URL is not in TSV, construct it from PATH if available
        relative_path = row.get('PATH', '')
        if not relative_path:
             # Fallback to older columns if PATH is missing
             relative_path = row.get('audio_url', row.get('mp3_url', ''))

        if relative_path and isinstance(relative_path, str) and not relative_path.startswith('http'):
             audio_url = f"https://cdn.freesound.org/mtg-jamendo/raw_30s/audio/{relative_path}"
        else:
             audio_url = relative_path
        
        if not track_id or not audio_url:
            continue
            
        # Check for internal railway URL if running locally
        if DATABASE_URL and 'railway.internal' in DATABASE_URL and 'RAILWAY_ENVIRONMENT' not in os.environ:
            logger.warning("Detected internal Railway DB URL while running locally. Connection will likely fail. Please use the TCP Proxy URL.")

        # 0. Check DB first (Optimization)
        if db_session:
            try:
                existing = db_session.query(Track).filter_by(id=track_id).first()
                if existing:
                    logger.info(f"Track {track_id} already in DB. Skipping.")
                    continue
            except Exception as e:
                logger.error(f"DB Check Error for {track_id}: {e}")
                # Optional: break or continue? Let's try to proceed if DB check fails, 
                # but with rollback just in case connection is dead
                db_session.rollback()

        filename = f"{track_id}.mp3"
        local_file_path = Path(output_dir) / filename
        s3_key = f"tracks/{filename}"
        
        logger.info(f"Processing Track {track_id}...")
        
        # 1. Download
        if not local_file_path.exists():
            success = download_file(audio_url, local_file_path)
            if not success:
               logger.error(f"Skipping {track_id} due to download failure.")
               continue
        
        # 2. Upload to S3
        if all([S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET_NAME]):
            final_url = upload_to_s3(s3_client, local_file_path, s3_key)
            if final_url:
                print(f"UPLOADED: {final_url}", flush=True)  # Force stdout for immediate log visibility
                # 3. Insert to DB
                if db_session:
                    try:
                        new_track = Track(
                            id=track_id,
                            title=f"Track {track_id}", # Placeholder if title missing
                            artist=str(row.get('artist_id', 'Unknown')),
                            tags={}, # Populate if available
                            audio_url=final_url
                        )
                        db_session.add(new_track)
                        db_session.commit()
                        logger.info(f"Track {track_id} metadata saved to DB.")
                    except Exception as e:
                        logger.error(f"DB Error for {track_id}: {e}")
                        db_session.rollback()
            
            # 4. Cleanup
            try:
                local_file_path.unlink()
                logger.info(f"Deleted local file {filename}")
            except Exception as e:
                logger.warning(f"Failed to delete {filename}: {e}")
        else:
             logger.warning("Skipping upload/DB (No credentials). Keeping local file.")

        processed_count += 1
        
    logger.info(f"Processing complete. {processed_count} tracks processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest MTG-Jamendo Dataset to S3")
    parser.add_argument("--tsv", required=True, help="Path to the dataset TSV file")
    parser.add_argument("--output", default="./temp_downloads", help="Directory to store downloaded files temporarily")
    parser.add_argument("--limit", type=int, help="Limit number of tracks to process")
    
    args = parser.parse_args()
    
    # Ensure models can be imported if running from root using `python data/ingest_mtg.py`
    # We might need to add backend to sys.path
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '../backend'))
    
    process_dataset(args.tsv, args.output, args.limit)
