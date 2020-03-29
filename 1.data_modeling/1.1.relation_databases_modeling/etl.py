import os
import glob
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import datetime
from tqdm.contrib import tenumerate
from sql_queries import (
    song_table_insert,
    artist_table_insert,
    songplay_table_insert,
    user_table_insert,
    time_table_insert,
    song_select,
)
from dotenv import load_dotenv
load_dotenv()


def process_song_file(cur, conn, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[
        ["song_id", "artist_id", "year", "title", "duration"]
    ].values.tolist()[0]

    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values.tolist()[0]

    # error due to duplicate rows ignore the row and rollback
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, conn, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit="ms")

    # insert time data records
    time_data = (
        t.values,
        t.dt.hour.values,
        t.dt.day.values,
        t.dt.weekofyear.values,
        t.dt.month.values,
        t.dt.year.values,
        t.dt.weekday.values,
    )
    column_labels = (
        "start_time",
        "hour",
        "day",
        "week",
        "month",
        "year",
        "weekday",
    )
    time_df = pd.DataFrame(
        data=dict(zip(column_labels, time_data)), columns=column_labels
    )
    time_df = time_df.drop_duplicates(subset="start_time")

    # insert time records
    execute_values(
        cur, time_table_insert, list(time_df.itertuples(index=False, name=None))
    )

    # load user table
    user_df = df.copy()
    user_df = user_df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df = user_df.drop_duplicates(subset="userId")

    # insert user records
    execute_values(
        cur, user_table_insert, list(user_df.itertuples(index=False, name=None))
    )

    # insert songplay records
    songplay_data = []
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        songid, artistid = None, None
        if results:
            songid, artistid = results

        # insert songplay record
        songplay_data.append(
            (
                datetime.datetime.fromtimestamp((row.ts // 1000)),
                int(row.userId),
                row.level,
                songid,
                artistid,
                int(row.sessionId),
                row.location,
                row.userAgent,
            )
        )
    execute_values(
        cur, songplay_table_insert, songplay_data,
    )


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in tenumerate(all_files, 1):
        func(cur, conn, datafile)
        conn.commit()


def main():
    conn = psycopg2.connect(
        f"host={os.getenv('DBHOST'):{os.getenv('DBPORT')}}\
        dbname={os.getenv('DBNAME')}\
        user={os.getenv('DBUSER')}\
        password={os.getenv('DBPASSWORD')}"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
