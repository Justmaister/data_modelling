import os
import glob
import csv
from cassandra.cluster import Cluster
from cassandra_queries import (
    create_song_length_table,
    create_song_playlist_session_table,
    create_users_song_table,
    insert_song_length,
    insert_song_playlist_session,
    insert_users_song
)

def process_event_data():
    filepath = os.getcwd() + '/event_data'
    full_data_rows_list = []

    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))

    for file in file_path_list:
        with open(file, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as file:
        writer = csv.writer(file, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',
                        'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                           row[7], row[8], row[12], row[13], row[16]))

def main():
    process_event_data()

    cluster = Cluster()
    session = cluster.connect()

    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkify
            WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
        """)
    except Exception as e:
        print(e)

    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)

    create_song_length_table(session)
    create_song_playlist_session_table(session)
    create_users_song_table(session)

    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            insert_song_length(session, line)
            insert_song_playlist_session(session, line)
            insert_users_song(session, line)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
