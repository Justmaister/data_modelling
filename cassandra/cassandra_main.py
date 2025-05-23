import os
import glob
import csv
import logging
from cassandra.cluster import Cluster
from cassandra_queries import (
    create_song_length_table,
    create_song_playlist_session_table,
    create_users_song_table,
    insert_song_length,
    insert_song_playlist_session,
    insert_users_song
)

CASSANDRA_CONFIG = {
    'keyspace': 'sparkify',
    'replication_factor': 1
}

DATA_DIR = os.path.join(os.getcwd(), 'event_data')
OUTPUT_FILE = 'event_datafile_new.csv'

CSV_COLUMNS = [
    'artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
    'level', 'location', 'sessionId', 'song', 'userId'
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CassandraManager:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect(self) -> None:
        """Establish connection to Cassandra cluster."""
        try:
            self.cluster = Cluster()
            self.session = self.cluster.connect()
            logger.info("Successfully connected to Cassandra cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise

    def create_keyspace(self) -> None:
        """Create keyspace if it doesn't exist."""
        try:
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_CONFIG['keyspace']}
                WITH REPLICATION = {{'class': 'SimpleStrategy',
                                   'replication_factor': {CASSANDRA_CONFIG['replication_factor']}}}
            """)
            logger.info(f"Keyspace {CASSANDRA_CONFIG['keyspace']} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create keyspace: {str(e)}")
            raise

    def set_keyspace(self) -> None:
        """Set the current keyspace."""
        try:
            self.session.set_keyspace(CASSANDRA_CONFIG['keyspace'])
            logger.info(f"Using keyspace: {CASSANDRA_CONFIG['keyspace']}")
        except Exception as e:
            logger.error(f"Failed to set keyspace: {str(e)}")
            raise

    def close(self) -> None:
        """Close Cassandra connection."""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("Cassandra connection closed")


def process_event_data() -> None:
    """Process event data files and create a consolidated CSV file."""
    try:
        full_data_rows_list = []

        file_path_list = []
        for root, _, files in os.walk(DATA_DIR):
            file_path_list.extend(glob.glob(os.path.join(root, '*')))

        for file in file_path_list:
            with open(file, 'r', encoding = 'utf8', newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)
                full_data_rows_list.extend(line for line in csvreader if line[0])

        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open(OUTPUT_FILE, 'w', encoding = 'utf8', newline='') as file:
            writer = csv.writer(file, dialect='myDialect')
            writer.writerow(CSV_COLUMNS)
            for row in full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                            row[7], row[8], row[12], row[13], row[16]))

        logger.info(f"Successfully processed {len(file_path_list)} files")

    except Exception as e:
            logger.error(f"Error processing event data: {str(e)}")
            raise

def main():
    """Main function to orchestrate the ETL process."""
    cassandra_manager = CassandraManager()
    try:

        process_event_data()

        cassandra_manager.connect()
        cassandra_manager.create_keyspace()
        cassandra_manager.set_keyspace()

        create_song_length_table(cassandra_manager.session)
        create_song_playlist_session_table(cassandra_manager.session)
        create_users_song_table(cassandra_manager.session)

        try:
            with open(OUTPUT_FILE, encoding = 'utf8') as file:
                csvreader = csv.reader(file)
                next(csvreader)

                for line in csvreader:
                    insert_song_length(cassandra_manager.session, line)
                    insert_song_playlist_session(cassandra_manager.session, line)
                    insert_users_song(cassandra_manager.session, line)

            logger.info("Successfully inserted data into Cassandra tables")

        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        cassandra_manager.close()


if __name__ == "__main__":
    main()
