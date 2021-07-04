def get_no_ghost() -> dict:
    return {'activity.per_year': {'$ne': {}}, 'is_bot': False}
