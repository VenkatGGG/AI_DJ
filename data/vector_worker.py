import boto3
from botocore.config import Config

import os
import time
import logging
import torch
import librosa
import numpy as np
import requests
from pathlib import Path
from tempfile import NamedTemporaryFile
from transformers import ClapModel, ClapProcessor
from dotenv import load_dotenv

import sys
# Ensure backend import works
sys.path.append(os.path.join(os.path.dirname(__file__), '../backend'))

from models import Track, get_db_session

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv(".env")

# S3 Configuration
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Model Configuration
MODEL_ID = "laion/clap-htsat-unfused"
DEVICE = "cpu" 

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        endpoint_url=S3_ENDPOINT_URL,
        config=Config(signature_version='s3v4')
    )

def get_presigned_url(s3_client, public_url):
    """Generates a presigned URL from the public URL stored in DB."""
    try:
        # Assuming URL format: .../BUCKET_NAME/key
        # or just extract key if we know the structure.
        # Structure from ingest: f"{S3_ENDPOINT_URL}/{S3_BUCKET_NAME}/{s3_key}"
        
        if not public_url:
            return None
            
        # Parse key. 
        # Robust way: use the exact known structure or split by bucket name
        if f"/{S3_BUCKET_NAME}/" in public_url:
            key = public_url.split(f"/{S3_BUCKET_NAME}/")[-1]
        else:
            # Fallback for paths that might differ or if bucket is subdomain
            # Ideally we stored key in DB, but we stored full URL.
            # Let's try splitting by 'tracks/' if standard
             if "tracks/" in public_url:
                 key = f"tracks/{public_url.split('tracks/')[-1]}"
             else:
                 return public_url # Cannot parse, try raw (will likely fail 403)

        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
            ExpiresIn=300 # 5 mins
        )
        return url
    except Exception as e:
        logger.error(f"Presigning failed: {e}")
        return public_url

def load_model():
    # ... (existing load_model code)
    logger.info(f"Loading CLAP model: {MODEL_ID} on {DEVICE}...")
    try:
        model = ClapModel.from_pretrained(MODEL_ID).to(DEVICE)
        processor = ClapProcessor.from_pretrained(MODEL_ID)
        logger.info("Model loaded successfully.")
        return model, processor
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        sys.exit(1)

def download_audio(url):
    """Downloads audio to a temporary file and returns path."""
    try:
        # ... (existing download_audio code)
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        suffix = ".mp3" # Force mp3 since we know it is, or derive from headers
        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
            return tmp_file.name
    except Exception as e:
        logger.error(f"Download failed for {url}: {e}")
        return None

def generate_embedding(model, processor, audio_path):
    """Generates audio embedding using CLAP."""
    try:
        # Load and resample audio
        # CLAP expects 48kHz usually (check config, but 48k is standard for CLAP)
        audio_array, sample_rate = librosa.load(audio_path, sr=48000)
        
        # Limit duration to 30s
        max_duration = 30
        if len(audio_array) > max_duration * sample_rate:
            audio_array = audio_array[:max_duration * sample_rate]

        # Process inputs
        inputs = processor(audios=audio_array, sampling_rate=48000, return_tensors="pt", padding=True)
        inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.get_audio_features(**inputs)
        
        # Retrieve embedding
        embedding = outputs[0].cpu().numpy().tolist()
        return embedding
    except Exception as e:
        logger.error(f"Inference failed for {audio_path}: {e}")
        return None

def process_queue():
    model, processor = load_model()
    session = get_db_session()
    s3_client = get_s3_client()
    
    if not session:
        logger.error("Could not connect to database.")
        sys.exit(1)

    logger.info("Starting Vector Worker Loop...")
    
    while True:
        try:
            track = session.query(Track).filter(Track.embedding == None).first()
            
            if not track:
                logger.info("No pending tracks found. Sleeping 10s...")
                time.sleep(10)
                session.expire_all()
                continue
                
            logger.info(f"Processing Track {track.id}...")
            
            # Generate Presigned URL
            download_url = get_presigned_url(s3_client, track.audio_url)
            
            # Download
            temp_path = download_audio(download_url)
            if not temp_path:
                logger.warning(f"Could not download audio for {track.id}.")
                time.sleep(5) 
                continue

            # ... (Rest of processing)
            # Embedding
            embedding = generate_embedding(model, processor, temp_path)
            
            # Cleanup temp file
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass

            if embedding:
                track.embedding = embedding
                try:
                    session.commit()
                    print(f"VECTORIZED: Track {track.id}", flush=True) 
                except Exception as e:
                    logger.error(f"DB Update failed: {e}")
                    session.rollback()
            else:
                 logger.warning(f"Embedding generation returned None for {track.id}")

            # yield execution slightly
            # time.sleep(0.1)

        except Exception as e:
            logger.error(f"Unexpected error in worker loop: {e}")
            session.rollback()
            time.sleep(5)

if __name__ == "__main__":
    process_queue()
