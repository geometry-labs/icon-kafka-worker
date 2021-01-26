def init_state(con):
    cur = con.cursor()
    cur.execute("SELECT reg_id, address, keyword, position from registrations")
    rows = cur.fetchall()

    state = {}

    for (_, address, keyword, position) in rows:
        if address not in state:
            state[address] = {keyword: position}
        else:
            state[address][keyword] = position

    return state
