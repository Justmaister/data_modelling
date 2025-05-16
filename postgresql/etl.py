import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    '''
     Read and transform data from song files and insert into songs and artists tables.
    '''
    df = pd.read_json(filepath, lines=True)

    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_columns].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    artist_columns = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Read and transform data from log files and insert into time, users and songplays tables.
    '''
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong']

    t = df.copy()
    t['ts'] = pd.to_datetime(t['ts'], unit='ms')

    time_data = [t.ts, t.ts.dt.hour.values, t.ts.dt.day.values, t.ts.dt.weekofyear.values, t.ts.dt.month.values, t.ts.dt.year.values, t.ts.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for index, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[["userId","firstName","lastName","gender","level"]]

    for index, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():

        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        start_time = pd.to_datetime(row.ts, unit='ms')

        if songid != None and artistid != None:
            songplay_data = ([start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent])
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Load all files from filepath, iterate over them and process
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for index, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(index, num_files))


def main():
    '''
        - Establishes connection with the sparkify database and gets cursor to it.
        - Process all song files
        - Process all log files
        - Finally, closes the connection.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()