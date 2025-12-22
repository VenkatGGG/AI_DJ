from fastapi import FastAPI
import os

app = FastAPI(title="Text2Tracks API")

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Text2Tracks Backend"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
