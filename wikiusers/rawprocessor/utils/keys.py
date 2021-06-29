class WhdtKeys:
    wiki_db = 0
    event_entity = 1
    event_type = 2
    event_timestamp = 3
    event_comment = 4
    event_user_id = 5
    event_user_text_historical = 6
    event_user_text = 7
    event_user_blocks_historical = 8
    event_user_blocks = 9
    event_user_groups_historical = 10
    event_user_groups = 11
    event_user_is_bot_by_historical = 12
    event_user_is_bot_by = 13
    event_user_is_created_by_self = 14
    event_user_is_created_by_system = 15
    event_user_is_created_by_peer = 16
    event_user_is_anonymous = 17
    event_user_registration_timestamp = 18
    event_user_creation_timestamp = 19
    event_user_first_edit_timestamp = 20
    event_user_revision_count = 21
    event_user_seconds_since_previous_revision = 22
    page_id = 23
    page_title_historical = 24
    page_title = 25
    page_namespace_historical = 26
    page_namespace_is_content_historical = 27
    page_namespace = 28
    page_namespace_is_content = 29
    page_is_redirect = 30
    page_is_deleted = 31
    page_creation_timestamp = 32
    page_first_edit_timestamp = 33
    page_revision_count = 34
    page_seconds_since_previous_revision = 35
    user_id = 36
    user_text_historical = 37
    user_text = 38
    user_blocks_historical = 39
    user_blocks = 40
    user_groups_historical = 41
    user_groups = 42
    user_is_bot_by_historical = 43
    user_is_bot_by = 44
    user_is_created_by_self = 45
    user_is_created_by_system = 46
    user_is_created_by_peer = 47
    user_is_anonymous = 48
    user_registration_timestamp = 49
    user_creation_timestamp = 50
    user_first_edit_timestamp = 51
    revision_id = 52
    revision_parent_id = 53
    revision_minor_edit = 54
    revision_deleted_parts = 55
    revision_deleted_parts_are_suppressed = 56
    revision_text_bytes = 57
    revision_text_bytes_diff = 58
    revision_text_sha1 = 59
    revision_content_model = 60
    revision_content_format = 61
    revision_is_deleted_by_page_deletion = 62
    revision_deleted_by_page_deletion_timestamp = 63
    revision_is_identity_reverted = 64
    revision_first_identity_reverting_revision_id = 65
    revision_seconds_to_identity_revert = 66
    revision_is_identity_revert = 67
    revision_is_from_before_page_creation = 68
    revision_tags = 69


EVENTS_MAP = {
    'create': 'create',
    'create-page': 'create',
    'delete': 'delete',
    'restore': 'restore',
    'move': 'move',
    'merge': 'merge',
    'edit': 'edit'
}
