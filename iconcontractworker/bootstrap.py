def init_state(con):
    """
    Downloads and populates a dictionary to be used as the local registration state
    :param con: psycopg2 connection object for the postgres database where the registration state is stored
    :return: dictionary containing the registration type in the form of {address: {keyword: position}}
    :rtype: dict
    """

    # SQL query
    cur = con.cursor()
    cur.execute("SELECT reg_id, address, keyword, position from registrations")
    rows = cur.fetchall()

    # Create & populate state dict
    state = {}

    for (_, address, keyword, position) in rows:
        if address not in state:
            state[address] = {keyword: position}
        else:
            state[address][keyword] = position

    return state
