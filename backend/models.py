from sqlalchemy import create_engine, Column, Integer, String, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
import os

Base = declarative_base()

class Track(Base):
    __tablename__ = 'tracks'
    
    id = Column(Text, primary_key=True)  # Can use string ID from dataset or UUID
    title = Column(Text)
    artist = Column(Text)
    tags = Column(JSONB)
    audio_url = Column(Text)
    # Using 512 dimensions for CLAP embeddings
    embedding = Column(Vector(512))
    semantic_id = Column(Text)

def init_db():
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("DATABASE_URL not set. Skipping DB init.")
        return
    
    # SQLAlchemy 1.4+ requires postgresql:// scheme
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(DATABASE_URL)
    
    # Create extension if not exists (requires superuser)
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()
    except Exception as e:
        print(f"Could not create vector extension (might need superuser): {e}")

    Base.metadata.create_all(engine)
    print("Database tables created.")

def get_db_session():
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        return None
        
    # SQLAlchemy 1.4+ requires postgresql:// scheme
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(".env") # Explicitly load from current dir
    print(f"DB URL found: {'Yes' if os.getenv('DATABASE_URL') else 'No'}")
    init_db()
