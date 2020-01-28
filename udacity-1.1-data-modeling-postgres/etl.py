import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes file and inserts song and artist information to their respective table
    
    Input:
    cur:Cursor object created by the database connection
    filepath:Filepath to the song json file
    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = list(artist_data)
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = list(song_data)
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
    Processes file and inserts time and user and information to their respective table.
    Artist and song information is queries from the database, and the aggregate data is inserted into the songplay table.
    
    Input:
    cur:Cursor object created by the database connection
    filepath:Filepath to the song json file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True, orient='columns')

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    datetimes = pd.to_datetime(t, unit='ms')
    
    # insert time data records
    time_data = [t, datetimes.dt.hour, datetimes.dt.day, datetimes.dt.week, datetimes.dt.month, datetimes.dt.year, datetimes.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, {'song': row.song, 'artist': row.artist, 'length': row.length})
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        #print(songplay_data)
        songplay_data = list(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes all .json files found in filepath (and subfolders) using the 'func' input parameter as a function
    
    Input:
    cur:Cursor object created by the database connection
    conn:The connection to the database
    filepath:Base filepath of the files to be loaded
    func:Function to be run for each file found, using the format: func(cur, datafile)
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Connects to the database, and inserts song and log information into facts and dimensional tables"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='./data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='./data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()