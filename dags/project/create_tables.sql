-- Drop existing tables if they exist
DROP TABLE IF EXISTS staging_events;
DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS songplays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS time;

-- Create staging table for events
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              CHAR(1),
    itemInSession       INT,
    lastName            VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            TEXT,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    sessionId           INT,
    song                VARCHAR,
    status              INT,
    ts                  BIGINT,
    userAgent           TEXT,
    userId              VARCHAR
);

-- Create staging table for songs
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     TEXT,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INT
);

-- Create fact table for songplays
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         VARCHAR     PRIMARY KEY,
    start_time          TIMESTAMP   NOT NULL,
    userid              VARCHAR,
    level               VARCHAR,
    song_id             VARCHAR,
    artist_id           VARCHAR,
    sessionid           INT,
    location            TEXT,
    useragent           TEXT
);

-- Create dimension table for users
CREATE TABLE IF NOT EXISTS users (
    userid              VARCHAR     PRIMARY KEY,
    first_name          VARCHAR,
    last_name           VARCHAR,
    gender              CHAR(1),
    level               VARCHAR
);

-- Create dimension table for songs
CREATE TABLE IF NOT EXISTS songs (
    song_id             VARCHAR     PRIMARY KEY,
    title               VARCHAR,
    artist_id           VARCHAR,
    year                INT,
    duration            FLOAT
);

-- Create dimension table for artists
CREATE TABLE IF NOT EXISTS artists (
    artist_id           VARCHAR     PRIMARY KEY,
    name                VARCHAR,
    location            TEXT,
    latitude            FLOAT,
    longitude           FLOAT
);

-- Create dimension table for time
CREATE TABLE IF NOT EXISTS time (
    start_time          TIMESTAMP   PRIMARY KEY,
    hour                INT,
    day                 INT,
    week                INT,
    month               INT,
    year                INT,
    weekday             INT
);
