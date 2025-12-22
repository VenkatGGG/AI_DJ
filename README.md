# Text2Tracks: Agentic Music Retrieval

A high-performance music recommendation engine comparing Generative Retrieval (Gemini 3) vs. Vector Retrieval (CLAP).

## Tech Stack
- **Frontend**: Next.js 15, TypeScript, Tailwind, Shadcn/UI.
- **Backend**: FastAPI, Python, PyTorch/CLAP.
- **Database**: PostgreSQL with `pgvector`.
- **Infrastructure**: Railway.

## Structure
- `/frontend`: Next.js Web App.
- `/backend`: FastAPI Server & Workers.
- `/data`: Scripts for data ingestion and processing.
