def new_user_insert_obj(uid: int, username: str, creation_timestamp, registration_timestamp, is_bot: bool, groups: list, blocks: list) -> dict:
        return {'id': uid, 'username': username, 'creation_timestamp': creation_timestamp, 'registration_timestamp': registration_timestamp, 'is_bot': is_bot, 'groups': groups, 'blocks': blocks, 'events': {'per_month': {}}}

def new_user_update_obj(uid: int, username: str, creation_timestamp, registration_timestamp, is_bot: bool, groups: list, blocks: list) -> dict:
    return {'id': uid, 'username': username, 'creation_timestamp': creation_timestamp, 'registration_timestamp': registration_timestamp, 'is_bot': is_bot, 'groups': groups, 'blocks': blocks}

def new_month_obj() -> list:
    return {'max_inact_interval': 0}

def two_digits(n: int) -> str:
    return str(n) if n > 9 else '0' + str(n)
