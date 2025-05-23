import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from typing import Callable, List, Tuple
import logging
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_song_file(cur: psycopg2.extensions.cursor, filepath: str) -> None:
    """
    Read and transform data from song files and insert into songs and artists tables.

    Args:
        cur: Database cursor
        filepath: Path to the song file
    """
    try:
        df = pd.read_json(filepath, lines=True)

        song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
        song_data = df[song_columns].values[0].tolist()
        cur.execute(song_table_insert, song_data)

        artist_columns = ['artist_id', 'artist_name', 'artist_location',
                         'artist_latitude', 'artist_longitude']
        artist_data = df[artist_columns].values[0].tolist()
        cur.execute(artist_table_insert, artist_data)

    except Exception as e:
        logger.error(f"Error processing song file {filepath}: {str(e)}")
        raise

def process_log_file(cur: psycopg2.extensions.cursor, filepath: str) -> None:
    """
    Read and transform data from log files and insert into time, users and songplays tables.

    Args:
        cur: Database cursor
        filepath: Path to the log file
    """
    try:
        df = pd.read_json(filepath, lines=True)
        df = df[df['page'] == 'NextSong']

        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        time_data = [
            df.ts,
            df.ts.dt.hour,
            df.ts.dt.day,
            df.ts.dt.isocalendar().week,
            df.ts.dt.month,
            df.ts.dt.year,
            df.ts.dt.weekday
        ]
        column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

        for _, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates()
        for _, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

        for _, row in df.iterrows():
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            songid, artistid = results if results else (None, None)
            start_time = pd.to_datetime(row.ts, unit='ms')

            if songid and artistid:
                songplay_data = [
                    start_time, row.userId, row.level, songid, artistid,
                    row.sessionId, row.location, row.userAgent
                ]
                cur.execute(songplay_table_insert, songplay_data)

    except Exception as e:
        logger.error(f"Error processing log file {filepath}: {str(e)}")
        raise

def process_data(cur: psycopg2.extensions.cursor,
                conn: psycopg2.extensions.connection,
                filepath: str,
                func: Callable) -> None:
    """
    Load all files from filepath, iterate over them and process.

    Args:
        cur: Database cursor
        conn: Database connection
        filepath: Path to the data directory
        func: Function to process each file
    """
    try:
        all_files = []
        for root, _, files in os.walk(filepath):
            files = glob.glob(os.path.join(root, '*.json'))
            all_files.extend(os.path.abspath(f) for f in files)

        num_files = len(all_files)
        logger.info(f'{num_files} files found in {filepath}')

        for index, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            logger.info(f'Processed {index}/{num_files} files')

    except Exception as e:
        logger.error(f"Error processing data in {filepath}: {str(e)}")
        raise

def get_db_connection() -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    """
    Create and return a database connection and cursor.

    Returns:
        Tuple containing database connection and cursor
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def main():
    """
    Main function to:
    - Establish database connection
    - Process all song files
    - Process all log files
    - Close the connection
    """
    try:
        conn, cur = get_db_connection()

        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()