import os
import glob
import psycopg2
import pandas as pd
from etl_prj.sql_queries import *
import numpy as np



def process_song_file(cur, filepath):
    """
    process song file, extract needed data and trasfer to target format and execute the insert sql for table song,artist
    :param cur: database cur
    :param filepath: song json dataset file path
    :return:None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     process log file, extract needed data and trasfer to target format and execute the insert sql for table: user,  songplay, time
    :param cur: database cur
    :param filepath: log file file path
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_nextsong = df['page'] == 'NextSong'
    df = df[is_nextsong]

    # load user table
    user_mdf = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    notnull = user_mdf['firstName'].notnull()
    user_df = user_mdf[notnull]
    user_df = user_df.drop_duplicates(subset='userId', keep="first")


    # insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except Exception as e:
        print(e, filepath, user_table_insert, row)

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records

    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({c: d for c, d in zip(column_labels, time_data)}).dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


    # insert songplay records
    for index, row in df.iterrows():
        str=song_select, (row.song, row.artist, row.length)

        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
            row.ts = pd.to_datetime(row.ts, unit='ms')

            # insert songplay record

            songplay_data = (
                row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


        else:
            songid, artistid = None, None










def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    # print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        # print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=Abc1234%")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='../../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../../data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()








