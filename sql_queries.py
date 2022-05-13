import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN= config.get("IAM_ROLE","ARN")
LOG_DATA= config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES
 
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""

    CREATE TABLE IF NOT EXISTS staging_events(
        artist             TEXT,
        auth               TEXT, 
        firstName          TEXT,
        gender             TEXT,
        itemInSession      INT, 
        lastName           TEXT,
        length             NUMERIC,
        level              TEXT,
        location           TEXT,
        method             TEXT,
        page               TEXT,
        registration       NUMERIC,
        sessionId          INT,
        song               TEXT,
        status             INT,
        ts                 BIGINT, 
        userAgent          TEXT,
        userId             INT
        );
        
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
    song_id            TEXT,
    num_songs          INT,
    title              TEXT,
    artist_name        TEXT,
    artist_latitude    DOUBLE PRECISION,
    artist_id          TEXT,
    year               INT,
    duration           NUMERIC,
    artist_longitude   DOUBLE PRECISION,
    artist_location    TEXT
    );
""")

# IDENTITY(seed,step). the index value starts from seed and increment by step. Type must be INT or BIGINT.
# Foreign key: we make sure these id columns matching to other dimension table id.  

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id    INT           IDENTITY(1,1) PRIMARY KEY,
    start_time     TIMESTAMP     NOT NULL      REFERENCES times(start_time) sortkey,
    user_id        INT           NOT NULL      REFERENCES users(user_id)   distkey,
    level          TEXT,
    song_id        TEXT                        REFERENCES songs(song_id),
    artist_id      TEXT                        REFERENCES artists(artist_id),
    session_id     INT,
    location       TEXT,
    user_agent     TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id        INT     PRIMARY KEY  distkey,  
    first_name     TEXT    NOT NULL,
    last_name      TEXT    NOT NULL,
    gender         TEXT,
    level          TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
    song_id       TEXT     PRIMARY KEY  distkey,
    title         TEXT     NOT NULL,
    artist_id     TEXT     NOT NULL    REFERENCES artists(artist_id),
    year          INT,
    duration      NUMERIC  NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id        TEXT          PRIMARY KEY  distkey,
    name             TEXT          NOT NULL,
    location         TEXT,
    latitude  DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS times (
    start_time      timestamp  PRIMARY KEY sortkey distkey,
    hour            SMALLINT,
    day             SMALLINT,
    week            SMALLINT,
    month           SMALLINT,
    year            SMALLINT,
    weekday         SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""

    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
    TIMESTAMP WITHOUT TIME ZONE 'epoch' + (se.ts / 1000) * INTERVAL '1 second', 
    se.userId, 
    se.level,
    ss.song_id, 
    ss.artist_id, 
    se.sessionId, 
    se.location, 
    se.userAgent            
    FROM staging_events AS se JOIN staging_songs AS ss
    ON ss.artist_name = se.artist AND ss.title = se.song    
    WHERE se.page = 'NextSong';

""")

user_table_insert = ("""

    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId)
           firstName,
           lastName,
           gender,
           level
    FROM staging_events
    WHERE page = 'NextSong';

""")

song_table_insert = ("""

    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT (song_id), 
           title, 
           artist_id, 
           year, 
           duration
    FROM staging_songs;
    
""")

artist_table_insert = ("""
    
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT (artist_id),
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude   
    FROM staging_songs;
    
""")

time_table_insert = ("""

    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT (sp.start_time), 
           EXTRACT(hour FROM sp.start_time),
           EXTRACT(day FROM sp.start_time),
           EXTRACT(week FROM sp.start_time),
           EXTRACT(month FROM sp.start_time),
           EXTRACT(year FROM sp.start_time),
           EXTRACT(weekday FROM sp.start_time)
    FROM songplays AS sp;
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
