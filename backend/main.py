from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
from .models import init_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB on startup
    print("Startup: Initializing Database...")
    init_db()
    yield

app = FastAPI(title="Text2Tracks API", lifespan=lifespan)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Text2Tracks Backend"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
