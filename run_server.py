"""Simple test server without uvicorn for debugging."""
import asyncio
from src.api_server import app

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting API server directly...")
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")
