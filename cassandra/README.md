# Project 1: Data modeling with Cassandra

## Summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of CSV logs on user activity on the app.

The purpose is to create a Cassandra database with tables designed to optimize specific queries on song play analysis, using a denormalized data model approach.

## Schema design and ETL pipeline

The Cassandra database is designed with three tables, each optimized for specific query patterns:

### Tables

#### song_length
Records song information for a specific session and item.

|   Column        |  Type  |
| --------------- | ------ |
| sessionId       | INT    |
| itemInSession   | INT    |
| artist          | TEXT   |
| song            | TEXT   |
| length          | FLOAT  |

Primary key: (sessionId, itemInSession)

#### song_playlist_session
Records user's song playlist information for a specific session.

|   Column        |  Type  |
| --------------- | ------ |
| userid          | INT    |
| sessionId       | INT    |
| itemInSession   | INT    |
| artist          | TEXT   |
| song            | TEXT   |
| firstName       | TEXT   |
| lastName        | TEXT   |

Primary key: ((userid, sessionId), itemInSession)

#### users_song
Records which users listened to which songs.

|   Column        |  Type  |
| --------------- | ------ |
| song            | TEXT   |
| userid          | INT    |
| first_name      | TEXT   |
| last_name       | TEXT   |

Primary key: (song, userid)

## Run

Run the scripts to create and populate the database:

```bash
python cassandra_main.py
```

## Sample queries

1. Find the artist, song title, and song length for sessionId = 338 and itemInSession = 4:
```sql
SELECT artist, song, length
FROM song_length
WHERE sessionId = 338 AND itemInSession = 4;
```

2. Find the name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10 and sessionid = 182:
```sql
SELECT artist, song, firstName, lastName
FROM song_playlist_session
WHERE userid = 10 AND sessionId = 182;
```

3. Find every user name (first and last) who listened to the song 'All Hands Against His Own':
```sql
SELECT first_name, last_name
FROM users_song
WHERE song = 'All Hands Against His Own';
```

## Design Decisions

1. **Denormalized Data Model**:
   - Each table is designed for a specific query pattern
   - Data is duplicated across tables to optimize read performance
   - No joins are needed, following Cassandra's best practices

2. **Partition Keys**:
   - `song_length`: Partitioned by sessionId for efficient session-based queries
   - `song_playlist_session`: Partitioned by (userid, sessionId) for user session analysis
   - `users_song`: Partitioned by song for song-based user analysis

3. **Clustering Columns**:
   - `itemInSession` is used as a clustering column to maintain order within partitions
   - This allows for efficient sorting and range queries within a session

