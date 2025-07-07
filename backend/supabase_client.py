# backend/supabase_client.py
import os
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path

# --- IMPORTANT ---
# This code now expects the .env file to be in the SAME directory as this script.
# So, your .env file MUST be inside the `backend/` folder.
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in the .env file in your project root")

supabase: Client = create_client(url, key)

# --- ADD THIS FUNCTION ---
# This is the dependency that main.py needs.
# It makes the global 'supabase' client available to FastAPI's Depends system.
def get_supabase_client() -> Client:
    return supabase