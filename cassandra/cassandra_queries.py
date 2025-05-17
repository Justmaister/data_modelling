def create_song_length_table(session):
    query = """
    CREATE TABLE IF NOT EXISTS song_length
    (sessionId INT, itemInSession INT, artist TEXT, song TEXT, length FLOAT,
    PRIMARY KEY (sessionId, itemInSession))
    """
    try:
        session.execute(query)
    except Exception as e:
        print(e)

def create_song_playlist_session_table(session):
    query = """
    CREATE TABLE IF NOT EXISTS song_playlist_session
    (userid INT, sessionId INT, itemInSession INT, artist TEXT, song TEXT,
    firstName TEXT, lastName TEXT,
    PRIMARY KEY ((userid, sessionid), itemInSession))
    """
    try:
        session.execute(query)
    except Exception as e:
        print(e)

def create_users_song_table(session):
    query = """
    CREATE TABLE IF NOT EXISTS users_song
    (song TEXT, userid INT, first_name TEXT, last_name TEXT,
    PRIMARY KEY (song,userid))
    """
    try:
        session.execute(query)
    except Exception as e:
        print(e)

def insert_song_length(session, line):
    query = """
    INSERT INTO song_length (sessionId, itemInSession, artist, song, length)
    VALUES (%s,%s,%s,%s,%s)
    """
    session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

def insert_song_playlist_session(session, line):
    query = """
    INSERT INTO song_playlist_session
    (userid, sessionId, itemInSession, artist, song, firstName, lastName)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    session.execute(query, (int(line[10]), int(line[8]), int(line[3]),
                          line[0], line[9], line[1], line[4]))

def insert_users_song(session, line):
    query = """
    INSERT INTO users_song (song, userid, first_name, last_name)
    VALUES (%s,%s,%s,%s)
    """
    session.execute(query, (line[9], int(line[10]), line[1], line[4]))