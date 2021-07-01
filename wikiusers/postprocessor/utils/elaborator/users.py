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
        'history': _create_history(input['alter'][key], key),
        'ever_had': list({ el for alter in input['alter'][key] for el in alter[key] })
    }


def _fill_usernames_history(input: dict, output: dict) -> None:
    output['usernames_history'] = _create_history(input['history_usernames'], 'username')


def _fill_activity(input: dict, output: dict) -> None:
    activity_per_month = {}
    activity_per_year = {}
    activity_total = {}

    # TOTAL HELPER VARS DECLARATION

    at_max_inactive_interval_in_days_sum = 0
    at_max_inactive_interval_in_days_count = 0
    at_max_inactive_interval_in_days_max = -1
    at_n_activity_days = 0
    at_sesc_since_same_day_event_sum = 0
    at_sesc_since_same_day_event_count = 0
    at_avg_secs_since_last_edit_on_same_page_sum = 0
    at_avg_secs_since_last_edit_on_same_page_count = 0
    at_avg_page_n_sum = 0
    at_avg_page_n_count = 0
    at_avg_page_entropy_sum = 0
    at_avg_page_entropy_count = 0

    events_per_month: dict[str, dict] = input['events']['per_month']
    for year, year_events in sorted(events_per_month.items()):
        activity_per_month[year] = {}

        # PER YEAR HELPER VARS DECLARATION

        activity_per_year[year] = {}
        apy = activity_per_year[year]
        apy_max_inactive_interval_in_days_sum = 0
        apy_max_inactive_interval_in_days_count = 0
        apy_max_inactive_interval_in_days_max = -1
        apy_n_activity_days = 0
        apy_sesc_since_same_day_event_sum = 0
        apy_sesc_since_same_day_event_count = 0
        apy_avg_secs_since_last_edit_on_same_page_sum = 0
        apy_avg_secs_since_last_edit_on_same_page_count = 0
        apy_avg_page_n_sum = 0
        apy_avg_page_n_count = 0
        apy_avg_page_entropy_sum = 0
        apy_avg_page_entropy_count = 0

        for month, month_details in sorted(year_events.items()):
            activity_per_month[year][month] = {}
            apm = activity_per_month[year][month]

            # PER MONTH PROPS

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
            for ns, ns_events in sorted(month_details['namespaces'].items()):
                apme_per_namespace[ns] = {}
                apme_curr_ns = apme_per_namespace[ns]

                total_ns = 0
                for event_type, event_count in sorted(ns_events.items()):
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

            # PER YEAR PROPS

            # max inactive interval

            apy_max_inactive_interval_in_days_sum += month_details['max_inact_interval']
            apy_max_inactive_interval_in_days_count += 1
            apy_max_inactive_interval_in_days_max = max(
                apy_max_inactive_interval_in_days_max, month_details['max_inact_interval'])

            # first/last timestamp

            if 'first_event_timestamp' not in apy:
                apy['first_event_timestamp'] = month_details['first_event']

            apy['last_event_timestamp'] = month_details['last_event']

            # n activity days

            apy_n_activity_days += month_details['activity_days']

            # secs since same day event

            apy_sesc_since_same_day_event_sum += month_details['secs_since_same_day_event']
            apy_sesc_since_same_day_event_count += month_details['secs_since_same_day_event_count']

            # secs since last edit on same page

            try:
                apy_avg_secs_since_last_edit_on_same_page_sum += month_details['pages_seconds']
                apy_avg_secs_since_last_edit_on_same_page_count += month_details['pages_seconds_count']
            except:
                pass

            # page n and entropy

            try:
                apy_avg_page_n_sum += month_details['pages']['n']
                apy_avg_page_n_count += 1

                apy_avg_page_entropy_sum += month_details['pages']['entropy']
                apy_avg_page_entropy_count += 1
            except:
                pass

            # TOTAL PROPS

            # first/last timestamp

            if 'first_event_timestamp' not in activity_total:
                activity_total['first_event_timestamp'] = month_details['first_event']

            activity_total['last_event_timestamp'] = month_details['last_event']

        # PER YEAR PROPS

        # max inactive interval

        apy['max_inactive_interval_in_days'] = apy_max_inactive_interval_in_days_max
        apy['avg_max_inactive_interval_in_days'] = _avg(
            apy_max_inactive_interval_in_days_sum, apy_max_inactive_interval_in_days_count)

        # n activity days

        apy['n_of_activity_days'] = apy_n_activity_days

        # secs since same day event

        apy['avg_secs_since_same_day_event'] = _avg(
            apy_sesc_since_same_day_event_sum, apy_sesc_since_same_day_event_count)

        # avg secs since last edit on same page

        apy['avg_secs_since_last_edit_on_same_page'] = _avg(
            apy_avg_secs_since_last_edit_on_same_page_sum, apy_avg_secs_since_last_edit_on_same_page_count)

        # page n and entropy

        apy['pages_activity'] = {
            'avg_n': _avg(
                apy_avg_page_n_sum, apy_avg_page_n_count),
            'avg_entropy': _avg(
                apy_avg_page_entropy_sum, apy_avg_page_entropy_count)
        }

        # events

        apy['events'] = {'per_namespace': {}, 'total': {}}
        apye = apy['events']
        apye_per_namespace = apye['per_namespace']
        apye_total = apye['total']
        for month, month_details in activity_per_month[year].items():
            apme_total = month_details['events']['total']
            apme_per_namespace = month_details['events']['per_namespace']

            for event_type, event_count in apme_total.items():
                try:
                    apye_total[event_type] += event_count
                except:
                    apye_total[event_type] = event_count

            for ns, ns_events in apme_per_namespace.items():
                if ns not in apye_per_namespace:
                    apye_per_namespace[ns] = {}
                apye_curr_ns = apye_per_namespace[ns]

                for event_type, event_count in ns_events.items():
                    try:
                        apye_curr_ns[event_type] += event_count
                    except:
                        apye_curr_ns[event_type] = event_count

        # TOTAL PROPS

        # max inactive interval

        at_max_inactive_interval_in_days_sum += apy_max_inactive_interval_in_days_sum
        at_max_inactive_interval_in_days_count += apy_max_inactive_interval_in_days_count
        at_max_inactive_interval_in_days_max = max(
            at_max_inactive_interval_in_days_max, apy_max_inactive_interval_in_days_max)

        # n activity days

        at_n_activity_days += apy_n_activity_days

        # secs since same day event

        at_sesc_since_same_day_event_sum += apy_sesc_since_same_day_event_sum
        at_sesc_since_same_day_event_count += apy_sesc_since_same_day_event_count

        # avg secs since last edit on same page

        at_avg_secs_since_last_edit_on_same_page_sum += apy_avg_secs_since_last_edit_on_same_page_sum
        at_avg_secs_since_last_edit_on_same_page_count += apy_avg_secs_since_last_edit_on_same_page_count

        # page n and entropy

        at_avg_page_n_sum += apy_avg_page_n_sum
        at_avg_page_n_count += apy_avg_page_n_count

        at_avg_page_entropy_sum += apy_avg_page_entropy_sum
        at_avg_page_entropy_count += apy_avg_page_entropy_count

    # TOTAL PROPS

    # max inactive interval

    activity_total['max_inactive_interval_in_days'] = at_max_inactive_interval_in_days_max
    activity_total['avg_max_inactive_interval_in_days'] = _avg(
        at_max_inactive_interval_in_days_sum, at_max_inactive_interval_in_days_count)

    # n activity days

    activity_total['n_activity_days'] = at_n_activity_days

    # secs since same day event

    activity_total['avg_secs_since_same_day_event'] = _avg(
        at_sesc_since_same_day_event_sum, at_sesc_since_same_day_event_count)

    # avg secs since last edit on same page

    activity_total['avg_secs_since_last_edit_on_same_page'] = _avg(
        at_avg_secs_since_last_edit_on_same_page_sum, at_avg_secs_since_last_edit_on_same_page_count)

    # page n and entropy

    activity_total['pages_activity'] = {
        'avg_n': _avg(
            at_avg_page_n_sum, at_avg_page_n_count),
        'avg_entropy': _avg(
            at_avg_page_entropy_sum, at_avg_page_entropy_count)
    }

    # events

    activity_total['events'] = {'per_namespace': {}, 'total': {}}
    ate = activity_total['events']
    ate_per_namespace = ate['per_namespace']
    ate_total = ate['total']
    for year, year_details in activity_per_year.items():
        apme_total = year_details['events']['total']
        apme_per_namespace = year_details['events']['per_namespace']

        for event_type, event_count in apme_total.items():
            try:
                ate_total[event_type] += event_count
            except:
                ate_total[event_type] = event_count

        for ns, ns_events in apme_per_namespace.items():
            if ns not in ate_per_namespace:
                ate_per_namespace[ns] = {}
            ate_curr_ns = ate_per_namespace[ns]

            for event_type, event_count in ns_events.items():
                try:
                    ate_curr_ns[event_type] += event_count
                except:
                    ate_curr_ns[event_type] = event_count

    # ADD CHAGES TO OUTPUT

    output['activity'] = {
        'total': activity_total,
        'per_year': activity_per_year,
        'per_month': activity_per_month
    }


def _elaborate_user(u: dict) -> dict:
    if u['username']:
        user = {}
        _fill_basic(u, user)
        _fill_groups_or_blocks(u, user, 'groups')
        _fill_groups_or_blocks(u, user, 'blocks')
        _fill_usernames_history(u, user)
        _fill_activity(u, user)
        return user
    else:
        return None


def elaborate_users_batch(users_batch: list[dict]) -> list[dict]:
    elaborated_users = [
        _elaborate_user(user)
        for user in users_batch
    ]
    return [
        user
        for user in elaborated_users
        if user is not None
    ]
