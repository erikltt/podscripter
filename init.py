#!/usr/bin/env python3
import csv
import gzip
import os
import sqlite3
from enum import Enum
from sqlite3 import Error
from urllib.parse import urlparse

import requests

IMDB_DATASET_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_DATASET_DIR = "imdb_dataset/"
DB_FILE = "moviedb.db"
IMDB_URLS = Enum("IMDB_URL", [("TITLES", "https://datasets.imdbws.com/title.basics.tsv.gz"),
                              ("TRANSLATION", "https://datasets.imdbws.com/title.akas.tsv.gz"),
                              ("RATING", "https://datasets.imdbws.com/title.ratings.tsv.gz")])

def __download_imdb_dataset(url=IMDB_URLS.TITLES.value):
    """Download IMDB Dataset and store it"""
    print("Downloading data...")
    url_parse = urlparse(url)
    filename = os.path.basename(url_parse.path)
    r = requests.get(url, allow_redirects=True)

    if not os.path.isdir(IMDB_DATASET_DIR):
        os.mkdir(IMDB_DATASET_DIR)
    folder_file = IMDB_DATASET_DIR + filename
    open(folder_file, 'wb').write(r.content)

    print("Ended")

    return folder_file


def loadtranslatedtitles_imdb():
    """Downloads and loads translated movie titles from IMDB downloaded dataset"""
    tsv_filename = __download_imdb_dataset(IMDB_URLS.TRANSLATION.value)

    # Open connection
    conn = create_connection(DB_FILE)

    with gzip.open(tsv_filename, "rt") as csvfile:
        # with open(input_tsv_file, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Updating data...")
        for row in csvreader:
            # if row["region"] == "FR" and row["language"] == "fr":
            if row["region"] == "FR":
                # Execute a SQL INSERT command
                movietranslation = (row["title"], row["titleId"])
                update_translation(conn, movietranslation)

        conn.commit()
    print("Ended")


def loadrating_imdb():
    """Downloads and loads movie ratings from IMDB downloaded dataset"""
    tsv_filename = __download_imdb_dataset(IMDB_URLS.RATING.value)

    # Open connection
    conn = create_connection(DB_FILE)

    with gzip.open(tsv_filename, "rt") as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Updating data...")
        for row in csvreader:
            # Execute a SQL UPDATE command
            movierating = (row["averageRating"], row["tconst"])
            update_ratings(conn, movierating)

        conn.commit()
    print("Ended")


def loadmovies_imdb():
    """Downloads and loads movie from IMDB downloaded dataset"""
    tsv_filename = __download_imdb_dataset()

    # Open connection
    conn = create_connection(DB_FILE)

    with gzip.open(tsv_filename, "rt") as csvfile:
        # with open(input_tsv_file, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        print("Inserting data...")
        for row in csvreader:
            if row["titleType"] == "movie":
                # Execute a SQL INSERT command
                movie = (row["originalTitle"], row["tconst"], row["originalTitle"], -1)
                create_movie(conn, movie)

        conn.commit()
    print("Ended")


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_index(conn, create_index_sql):
    try:
        c = conn.cursor()
        c.execute(create_index_sql)
    except Error as e:
        print(e)


def create_movie(conn, movie):
    sql = ''' INSERT INTO movie(title, imdbid, translated, rating)
                  VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, movie)
    conn.commit()


def update_ratings(conn, movierating):
    sql = '''UPDATE movie set rating=? where imdbid=?'''

    cur = conn.cursor()
    cur.execute(sql, movierating)
    conn.commit()


def update_translation(conn, movie):
    sql = '''UPDATE movie set TRANSLATED=? where imdbid=?'''

    cur = conn.cursor()
    cur.execute(sql, movie)
    conn.commit()


if __name__ == '__main__':
    sql_movie_table = """
    CREATE TABLE movie (
    id integer PRIMARY KEY AUTOINCREMENT,
    title text NOT NULL,
    imdbid text NOT NULL,
    translated text NOT NULL,
    rating real DEFAULT -1
    );"""

    sql_movie_index_imdb = """
    CREATE UNIQUE INDEX idx_movieid
    ON movie(imdbid);
    """

    sql_movie_index_title = """
        CREATE INDEX movie_title_idx
        ON movie(title);
        """

    db_connection = create_connection(DB_FILE)
    create_table(db_connection, sql_movie_table)
    create_index(db_connection, sql_movie_index_imdb)
    create_index(db_connection, sql_movie_index_title)

    loadrating_imdb()