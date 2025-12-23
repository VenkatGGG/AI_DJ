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
from sqlalchemy import text

import sys
# Ensure backend import works
sys.path.append(os.path.join(os.path.dirname(__file__), '../backend'))

from models import Track, get_db_session

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv(".env")

# Model Configuration
MODEL_ID = "laion/clap-htsat-unfused"
DEVICE = "cpu" # Force CPU for Railway cost/compatibility

def load_model():
    logger.info(f"Loading CLAP model: {MODEL_ID} on {DEVICE}...")
    try:
        model = ClapModel.from_pretrained(MODEL_ID).to(DEVICE)
        processor = ClapProcessor.from_pretrained(MODEL_ID)
        logger.info("Model loaded successfully.")
        return model, processor
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        # In a real worker, we might want to exit and let the supervisor restart
        sys.exit(1)

def download_audio(url):
    """Downloads audio to a temporary file and returns path."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        suffix = Path(url).suffix or ".mp3"
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
    # Only load model once
    model, processor = load_model()
    session = get_db_session()
    
    if not session:
        logger.error("Could not connect to database.")
        sys.exit(1)

    logger.info("Starting Vector Worker Loop...")
    
    while True:
        try:
            # Find track with NULL embedding
            # Postgres specific: using FOR UPDATE SKIP LOCKED is better for concurrency,
            # but for MVP single worker this is fine.
            track = session.query(Track).filter(Track.embedding == None).first()
            
            if not track:
                logger.info("No pending tracks found. Sleeping 10s...")
                time.sleep(10)
                session.expire_all()
                continue
                
            logger.info(f"Processing Track {track.id}...")
            
            # Download
            temp_path = download_audio(track.audio_url)
            if not temp_path:
                logger.warning(f"Could not download audio for {track.id}.")
                # Skip logic: Just continue? Ideally verify if URL is broken.
                # For now prevent infinite loop on same track by sleeping
                time.sleep(5) 
                # Ideally we should flag it, but schema doesn't have status col yet.
                # Let's hope it's transient.
                continue

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
                    print(f"VECTORIZED: Track {track.id}", flush=True) # Visible Log
                except Exception as e:
                    logger.error(f"DB Update failed: {e}")
                    session.rollback()
            else:
                 logger.warning(f"Embedding generation returned None for {track.id}")

            # Optional: Sleep slightly to prevent CPU hogging if running on shared?
            # Railway vCPU is fair usage. Fast is fine.

        except Exception as e:
            logger.error(f"Unexpected error in worker loop: {e}")
            session.rollback()
            time.sleep(5)

if __name__ == "__main__":
    process_queue()
