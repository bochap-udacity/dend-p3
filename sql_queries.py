# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

staging_events_table_create = """
  CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(255),
    auth VARCHAR(15),
    firstName VARCHAR(25),
    gender VARCHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(25),
    length DOUBLE PRECISION,
    level VARCHAR(5),
    location VARCHAR(50),
    method VARCHAR(10),
    page VARCHAR(20),
    registration DOUBLE PRECISION,
    session_id INTEGER, 
    song VARCHAR(255),
    status INTEGER,
    ts TIMESTAMP distkey, 
    userAgent VARCHAR(255),
    userId INTEGER
  );
"""

staging_songs_table_create = """
  CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR(25) distkey,
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_longitude DOUBLE PRECISION,
    artist_name VARCHAR(255),
    duration DOUBLE PRECISION,
    num_songs INTEGER,
    song_id VARCHAR(25), 
    title VARCHAR(255),
    year INTEGER 
  );
"""

songplay_table_create = """
  CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(1,1) NOT NULL,
    start_time timestamp NOT NULL, 
    user_id INTEGER NOT NULL, 
    level VARCHAR(5) NOT NULL, 
    song_id VARCHAR(25) distkey,  
    artist_id VARCHAR(25),  
    session_id INTEGER NOT NULL,  
    location VARCHAR(50) NOT NULL, 
    user_agent VARCHAR(255) NOT NULL,
    primary key(songplay_id),
    foreign key(start_time) references time(start_time),
    foreign key(user_id) references users(user_id),
    foreign key(song_id) references songs(song_id),
    foreign key(artist_id) references artists(artist_id)
  )
  diststyle key
  compound sortkey(user_id, artist_id, start_time);
"""

user_table_create = """
  CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER NOT NULL distkey sortkey,
    first_name VARCHAR(25) NOT NULL, 
    last_name VARCHAR(25) NOT NULL,
    gender VARCHAR(1) NOT NULL, 
    level VARCHAR(5) NOT NULL,
    primary key(user_id)
  );
"""

song_table_create = """
  CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(25) NOT NULL distkey, 
    title VARCHAR(255), 
    artist_id VARCHAR(25) NOT NULL, 
    year INTEGER NOT NULL, 
    duration DOUBLE PRECISION NOT NULL,
    primary key(song_id)
  );
"""

artist_table_create = """
  CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(25) NOT NULL distkey,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    primary key(artist_id)
  );
"""

time_table_create = """
  CREATE TABLE time (
    start_time timestamp NOT NULL distkey sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday INTEGER NOT NULL,
    primary key(start_time)
  );
"""

# STAGING TABLES
staging_events_copy = """
  COPY staging_events FROM '{}'
  CREDENTIALS 'aws_iam_role={}'
  REGION '{}' 
  COMPUPDATE OFF STATUPDATE OFF
  FORMAT AS JSON '{}'
  TIMEFORMAT as 'epochmillisecs';
"""

staging_songs_copy = """
  COPY staging_songs FROM '{}'
  CREDENTIALS 'aws_iam_role={}'
  REGION '{}'
  COMPUPDATE OFF STATUPDATE OFF  
  FORMAT AS JSON 'auto';
"""

# FINAL TABLES

songplay_table_insert = """
  INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  )
  SELECT
    e.ts, e.userId, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.userAgent
  FROM
    staging_events e
    INNER JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
  GROUP BY 
    e.ts, e.userId, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.userAgent
"""

user_table_insert = """
  INSERT INTO users (
    user_id, first_name, last_name, gender, level
  )
  SELECT
    userId, firstName, lastName, gender, level
  FROM
    staging_events
  WHERE 
    page = 'NextSong'
  GROUP BY 
    userId, firstName, lastName, gender, level
"""

song_table_insert = """
  INSERT INTO songs (
    song_id, title, artist_id, year, duration
  )
  SELECT
    song_id, title, artist_id, year, duration
  FROM
    staging_songs
  GROUP BY 
    song_id, title, artist_id, year, duration
"""

artist_table_insert = """
  INSERT INTO artists (
    artist_id, name, location, latitude, longitude   
  )
  SELECT
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM
    staging_songs
  GROUP BY 
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
"""

time_table_insert = """
  INSERT INTO time (
    start_time, 
    hour, day, week, 
    month, year, weekday
  )
  WITH event_time AS (
    SELECT 
      DISTINCT start_time
    FROM 
      songplays
  )
  SELECT
    start_time, 
    EXTRACT(hour from start_time), EXTRACT(day from start_time), 
    EXTRACT(week from start_time), EXTRACT(month from start_time), 
    EXTRACT(year from start_time), EXTRACT(dow from start_time)
  FROM event_time
"""
# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,    
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

def copy_table_queries(iam_role_arn, region, log_data, log_jsonpath, song_data):
  """
    Using a parameterized method so that the resource identifies are passed
  """
  return [
    staging_events_copy.format(log_data, iam_role_arn, region, log_jsonpath),
    staging_songs_copy.format(song_data, iam_role_arn, region)
  ]

insert_table_queries = [
    songplay_table_insert,  
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
