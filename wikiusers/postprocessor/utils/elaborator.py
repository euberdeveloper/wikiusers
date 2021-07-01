def _avg(total: float, count: float) -> float:
    try:
        return total / count
    except ZeroDivisionError:
        return 0


def _fill_basic(input: dict, output: dict) -> None:
    output['id'] = input['id']
    output['username'] = input['username']
    output['creation_timestamp'] = input['creation_timestamp']
    output['registration_timestamp'] = input['registration_timestamp']
    output['is_bot'] = input['is_bot']


def _create_history(alter_groups: list[dict], key: str) -> list[dict]:
    history = []
    for i, alter in enumerate(sorted(alter_groups, key=lambda el: el['timestamp'])):
        timestamp = alter['timestamp']
        history.append({'from': timestamp, 'to': None, key: alter[key]})
        if i > 0:
            history[i - 1]['to'] = timestamp
    return history


def _fill_groups_or_blocks(input: dict, output: dict, key: str) -> None:
    output[key] = {
        'current': input[key],
        'history': _create_history(input['alter'][key], key)
    }


def _fill_usernames_history(input: dict, output: dict) -> None:
    output['usernames_history'] = _create_history(input['history_usernames'], 'username')


def _fill_activity(input: dict, output: dict) -> None:
    activity_per_month = {}
    activity_per_year = {}
    activity_total = {}
    
    events_per_month: dict[str, dict] = input['events']['per_month']
    for year, year_events in events_per_month.items():
        activity_per_month[year] = {}

        activity_per_year[year] = {}
        apy = activity_per_year[year]

        for month, month_details in year_events.items():
            activity_per_month[year][month] = {}
            apm = activity_per_month[year][month]

            apm['max_inactive_interval_in_days'] = month_details['max_inact_interval']
            apm['first_event_timestamp'] = month_details['first_event']
            apm['last_event_timestamp'] = month_details['last_event']
            apm['n_of_activity_days'] = month_details['activity_days']
            apm['avg_secs_since_same_day_event'] = _avg(
                month_details['secs_since_same_day_event'], 
                month_details['secs_since_same_day_event_count']
            )
            try:
                apm['avg_secs_since_last_edit_on_same_page'] = _avg(
                    month_details['pages_seconds'], 
                    month_details['pages_seconds_count']
                )
            except:
                pass
            
            try:
                apm['pages_activity'] = month_details['pages']
            except:
                pass

            apm['events'] = {'per_namespace': {}, 'total': {}}
            apme = apm['events']
            apme_per_namespace = apme['per_namespace']
            apme_total = apme['total']
            for ns, ns_events in month_details['namespaces'].items():
                apme_per_namespace[ns] = {}
                apme_curr_ns = apme_per_namespace[ns]

                total_ns = 0
                for event_type, event_count in ns_events.items():
                    apme_curr_ns[event_type] = event_count
                    if event_type != 'minor_edit':
                        total_ns += event_count
                    try:
                        apme_total[event_type] += event_count
                    except:
                        apme_total[event_type] = event_count

                apme_curr_ns['total'] = total_ns
                try:
                    apme_total['total'] += total_ns
                except:
                    apme_total['total'] = total_ns

    output['activity'] = {
        'total': activity_total,
        'per_year': activity_per_year,
        'per_month': activity_per_month
    }


def _elaborate_user(u: dict) -> dict:
    user = {}
    _fill_basic(u, user)
    _fill_groups_or_blocks(u, user, 'groups')
    _fill_groups_or_blocks(u, user, 'blocks')
    _fill_usernames_history(u, user)
    _fill_activity(u, user)
    return user


def elaborate_users_batch(users_batch: list[dict]) -> list[dict]:
    return [
        _elaborate_user(user)
        for user in users_batch
    ]
