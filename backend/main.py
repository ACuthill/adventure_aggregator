# backend/main.py

from fastapi import FastAPI, Depends, HTTPException, status
from typing import List, Optional
from supabase import Client

# --- NEW IMPORT ---
from fastapi.middleware.cors import CORSMiddleware

from . import crud, schemas
from .supabase_client import get_supabase_client

app = FastAPI()

# --- ADD THIS MIDDLEWARE SECTION ---
# This list defines which origins are allowed to make requests to your API.
origins = [
    "http://localhost",
    "http://127.0.0.1",
    "null",  # This is crucial for allowing 'file://' access
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # The list of origins that are allowed to make requests.
    allow_credentials=True,      # Support for cookies.
    allow_methods=["*"],         # Allow all methods (GET, POST, etc.).
    allow_headers=["*"],         # Allow all headers.
)
# ---------------------------------

@app.get("/")
def read_root():
    return {"message": "Welcome to the Adventure Aggregator API!"}

# The rest of your endpoints are unchanged...

@app.post("/adventures/", response_model=schemas.Adventure, status_code=status.HTTP_200_OK)
def create_or_update_adventure(
    adventure: schemas.AdventureCreate,
    supabase: Client = Depends(get_supabase_client)
):
    upserted_adventure = crud.upsert_adventure(supabase=supabase, adventure=adventure)
    if not upserted_adventure:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to create or update adventure."
        )
    return upserted_adventure


@app.get("/adventures/", response_model=List[schemas.Adventure])
def read_adventures(
    limit: int = 20,
    offset: int = 0,
    sort_by: Optional[str] = "departure_date",
    order: Optional[str] = "asc",
    activity_type: Optional[str] = None,
    location: Optional[str] = None,
    supabase: Client = Depends(get_supabase_client)
):
    adventures = crud.get_filtered_adventures(
        supabase,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        order=order,
        activity_type=activity_type,
        location=location
    )
    return adventures