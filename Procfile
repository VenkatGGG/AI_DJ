web: uvicorn backend.main:app --host 0.0.0.0 --port $PORT
worker: python data/ingest_mtg.py --tsv data/dataset.tsv --output /tmp
