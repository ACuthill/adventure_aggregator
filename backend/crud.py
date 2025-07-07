from supabase import Client
from . import schemas

def get_filtered_adventures(
    supabase: Client,
    limit: int,
    offset: int,
    sort_by: str,
    order: str,
    activity_type: str = None,
    location: str = None
):
    """
    Fetches adventures with full filtering, sorting, and pagination.
    """
    query = supabase.table("adventures").select("*")

    # --- Filtering ---
    # This logic applies filters if they are provided in the URL query.
    if activity_type:
        query = query.ilike("activity_type", f"%{activity_type}%")
    if location:
        query = query.ilike("location", f"%{location}%")

    # --- Sorting ---
    # Determine the sorting direction. The Supabase library uses 'desc=True' for
    # descending order and 'desc=False' (or omitting it) for ascending order.
    is_descending = (order.lower() == 'desc')
    
    # Whitelist the columns we allow sorting on to prevent misuse.
    # Then apply the .order() method with the correct 'desc' keyword.
    if sort_by in ["price", "duration", "departure_date"]:
        query = query.order(sort_by, desc=is_descending)

    # --- Pagination ---
    # The .range() method tells the database which slice of results to return.
    # This must be applied after filtering and sorting.
    query = query.range(offset, offset + limit - 1)
    
    # --- Execute ---
    # Finally, execute the fully constructed query.
    response = query.execute()

    if not response.data:
        return []
    return response.data


def upsert_adventure(supabase: Client, adventure: schemas.AdventureCreate):
    """
    Inserts a new adventure or updates an existing one based on its unique_id.
    
    Args:
        supabase: The Supabase client instance.
        adventure: A Pydantic schema object with the adventure data.
    """
    # Use mode='json' to ensure special types like datetime are converted
    # to JSON-serializable strings before sending them to the Supabase client.
    adventure_dict = adventure.model_dump(mode='json')
    
    # The .upsert() method is the core of our write operation.
    # It checks the primary key ('unique_id') to decide whether to INSERT or UPDATE.
    response = supabase.table("adventures").upsert(adventure_dict).execute()

    # The response.data will contain a list with the newly created or updated record.
    if response.data:
        return response.data[0]
    return None