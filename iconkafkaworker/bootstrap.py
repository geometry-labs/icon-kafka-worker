from iconkafkaworker.settings import settings


def init_log_registration_state(con):
    """
    Downloads and populates a dictionary to be used as the local registration state
    :param con: psycopg2 connection object for the postgres database where the registration state is stored
    :return: dictionary containing the registration type in the form of {address: {keyword: position}}
    :rtype: dict
    """

    # SQL query
    cur = con.cursor()
    cur.execute(
        "SELECT reg_id, to_address, keyword, position from {} WHERE type = 'logevent'".format(
            settings.registrations_topic
        )
    )
    rows = cur.fetchall()

    # Create & populate state dict
    log_events_state = {}
    reverse_search = {}

    for (reg_id, address, keyword, position) in rows:

        reverse_search[reg_id] = (address, keyword, position)

        if address not in log_events_state:
            log_events_state[address] = {keyword: {position: [reg_id]}}
        else:
            if keyword not in log_events_state[address]:
                log_events_state[address][keyword] = {position: [reg_id]}
            else:
                if position not in log_events_state[address][keyword]:
                    log_events_state[address][keyword][position] = [reg_id]
                else:
                    log_events_state[address][keyword][position].append(reg_id)

    broadcaster_events_pairs = {}

    cur = con.cursor()
    cur.execute(
        "SELECT broadcaster_id, event_id from {}".format(
            settings.broadcaster_events_table
        )
    )
    rows = cur.fetchall()

    for (broadcaster, event) in rows:
        broadcaster_events_pairs[event] = broadcaster

    return log_events_state, broadcaster_events_pairs, reverse_search


def init_tx_registration_state(con):
    """
    Downloads and populates a dictionary to be used as the local registration state
    :param con: psycopg2 connection object for the postgres database where the registration state is stored
    :return: dictionary containing the registration type in the form of {address: {keyword: position}}
    :rtype: dict
    """

    # SQL query
    cur = con.cursor()
    cur.execute(
        "SELECT reg_id, from_address, to_address, value FROM registrations WHERE type = 'trans'"
    )
    rows = cur.fetchall()

    # Create & populate state dict
    address_pairs_state = {}
    reverse_search = {}

    for (reg_id, from_address, to_address, _) in rows:

        reverse_search[reg_id] = (to_address, from_address)

        if not from_address:
            tmp_from = "*"
        else:
            tmp_from = from_address

        if not to_address:
            tmp_to = "*"
        else:
            tmp_to = to_address

        if tmp_to not in address_pairs_state:
            address_pairs_state[tmp_to] = {tmp_from: [reg_id]}
        else:
            if tmp_from not in address_pairs_state[tmp_to]:
                address_pairs_state[tmp_to][tmp_from] = [reg_id]
            else:
                address_pairs_state[tmp_to][tmp_from].append(reg_id)

    broadcaster_events_pairs = {}

    cur = con.cursor()
    cur.execute(
        "SELECT broadcaster_id, event_id from {}".format(
            settings.broadcaster_events_table
        )
    )
    rows = cur.fetchall()

    for (broadcaster, event) in rows:
        broadcaster_events_pairs[event] = broadcaster

    return address_pairs_state, broadcaster_events_pairs, reverse_search
