#!/usr/bin/env python3
import csv
import gzip
import json
import pathlib
import time
import uuid
from enum import Enum
from os import listdir
from os.path import isfile, join
from urllib.parse import urlparse

import psycopg2 as psycopg2
import requests
import spacy as spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span
from vosk import Model, KaldiRecognizer, SetLogLevel
import sys
import os
import wave
import subprocess
from pydub import AudioSegment
from pydub.silence import split_on_silence
import argparse
from spacy.matcher import Matcher

import feedparser

AUDIO_FILE = "converted.wav"
TEXT_FILE = "transcripted.txt"
AUDIO_CHUNKS_FOLDER = "audio-chunks"
SAMPLE_RATE = 16000

HOST = "localhost"
USER = "openlibdbworks"
PASSWORD = "120485"
DATABASE = "openlibdbworks"
IMDB_DATASET_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_DATASET_DIR = "imdb_dataset/"
PODCAST_DIR = "podcasts/"
IMDB_URLS = Enum("IMDB_URL", [("TITLES", "https://datasets.imdbws.com/title.basics.tsv.gz"),
                          ("TRANSLATION", "https://datasets.imdbws.com/title.akas.tsv.gz")])


def download_imdb_dataset(url=IMDB_URLS.TITLES.value):
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


def download_rss_feed():
    feed = feedparser.parse(xml_feed_url)

    for entry in feed.entries:
        url = entry.links[1].href
        url_parse = urlparse(url)
        filename = os.path.basename(url_parse.path)
        r = requests.get(url, allow_redirects=True)

        if not os.path.isdir(PODCAST_DIR):
            os.mkdir(PODCAST_DIR)

        folder_file = PODCAST_DIR + filename

        open(folder_file, 'wb').write(r.content)


# a function that splits the audio file into chunks
# and applies speech recognition
def chunk_wav_file(wav_file, folder=AUDIO_CHUNKS_FOLDER):
    """
    Splitting the large audio file into chunks
    """
    # open the audio file using pydub
    sound = AudioSegment.from_wav(wav_file)
    # split audio sound where silence is 700 miliseconds or more and get chunks
    print("Splitting file...")
    chunks = split_on_silence(sound,
                              # experiment with this value for your target audio file
                              min_silence_len=800,
                              # adjust this per requirement
                              silence_thresh=sound.dBFS - 14,
                              # keep the silence for 1 second, adjustable as well
                              keep_silence=100,
                              )
    # create a directory to store the audio chunks
    if not os.path.isdir(folder):
        os.mkdir(folder)

    # process each chunk
    for i, audio_chunk in enumerate(chunks, start=1):
        # export audio chunk and save it in
        # the `folder_name` directory.
        progress(i, len(chunks), "Writing chunks")
        chunk_filename = os.path.join(folder, f"chunk{i:04}.wav")
        audio_chunk.export(chunk_filename, format="wav")

    print("Ended")

    return audio_chunk


def vosk_capture(model, recorder, audiofile_path):
    SetLogLevel(0)

    process = subprocess.Popen(['ffmpeg', '-loglevel', 'quiet', '-i',
                                audiofile_path,
                                '-ar', str(SAMPLE_RATE), '-ac', '1', '-f', 's16le', '-'],
                               stdout=subprocess.PIPE)

    while True:
        data = process.stdout.read(4000)
        if len(data) == 0:
            break
        else:
            recorder.AcceptWaveform(data)
            # print(recorder.Result())

    return json.loads(recorder.FinalResult())["text"]


def speed_change(audiofile, speed=1.0):
    # Manually override the frame_rate. This tells the computer how many
    # samples to play per second
    sound_with_altered_frame_rate = audiofile._spawn(audiofile.raw_data, overrides={
        "frame_rate": int(audiofile.frame_rate * speed)
    })

    # convert the sound with altered frame rate to a standard frame rate
    # so that regular playback programs will work right. They often only
    # know how to play audio at standard frame rate (like 44.1k)
    return sound_with_altered_frame_rate.set_frame_rate(audiofile.frame_rate)


def sound_convert_to_wav(mp3_filepath):
    filename, file_extension = os.path.splitext(mp3_filepath)
    # convert mp3 to wav
    sound = AudioSegment.from_mp3(mp3_filepath)
    # sound = speed_change(sound, 0.5)
    wav_file_path = filename + ".wav"
    sound.export(wav_file_path, format="wav")

    return wav_file_path


def write_line(text_to_write, name_of_file=TEXT_FILE):
    # Open the file in append & read mode ('a+')
    with open(name_of_file, "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(text_to_write)


def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


def transcription():
    if not os.path.exists("model"):
        print(
            "Please download the model from https://alphacephei.com/vosk/models and unpack as 'model' in the current folder.")
        exit(1)
    model = Model("model")
    rec = KaldiRecognizer(model, SAMPLE_RATE)

    filename = pathlib.PurePath(chunk_folder).name + ".txt"
    list_of_chunks = [f for f in sorted(listdir(chunk_folder)) if isfile(join(chunk_folder, f))]

    # filename = time.strftime("%Y%m%d-%H%M%S") + ".txt"
    for i, chunk_filename in enumerate(list_of_chunks, start=1):
        # process each chunk
        progress(i, len(list_of_chunks), "Transcribing")
        text_transcripted = vosk_capture(model, rec, chunk_folder + '/' + chunk_filename)
        if text_transcripted != "":
            write_line(text_transcripted, filename)

    print("\nEnded")


def conversion():
    chunks = []
    wav_file = sound_convert_to_wav(sound_file_path)
    folder = os.path.splitext(sound_file_path)[0]
    chunks = chunk_wav_file(wav_file, folder)


def database_extraction():
    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()
    # table full scan to load data
    rs = []
    rs_string = []
    sql = 'select translated from movie'
    cur.execute(sql)
    rs.append(cur.fetchall())

    return rs


def match_film(doc, matcher, pattern, position):
    match_list = []
    matcher.add("FILM", [pattern])
    matches = matcher(doc)
    matcher.remove("FILM")

    for match_id, start, end in matches:
        match_list.append(str.replace(doc[start + position].text, '_', ' '))

    return match_list


def parse():
    with open(transcripted_file) as f:
        lines = f.readlines()

    f.close()
    transcripted_text = ' '.join(lines)

    rs = database_extraction()
    rs_string = []
    for title in rs[0]:
        text_title = title[0].lower()
        if len(text_title) > 2 and text_title in transcripted_text:
            replaced_title = str.replace(text_title, ' ', '_').lower()
            rs_string.append(replaced_title)
            transcripted_text = str.replace(transcripted_text, text_title, replaced_title)

    nlp = spacy.load('/home/sylvain/.local/lib/python3.8/site-packages/fr_core_news_sm/fr_core_news_sm-3.2.0')

    matcher = Matcher(nlp.vocab)
    doc = nlp(transcripted_text)
    match_list = []

    match_list.extend(
        match_film(doc, matcher,
                   [{"ENT_TYPE": "PER"},
                    {"TEXT": "dans"},
                    {"LOWER": {"IN": rs_string}}], 2))
    match_list.extend(
        match_film(doc, matcher,
                   [{"TEXT": "en", "OP": "!"},
                    {"LOWER": {"IN": rs_string}},
                    {"TEXT": "de"},
                    {"ENT_TYPE": "PER"}], 1))
    match_list.extend(
        match_film(doc, matcher,
                   [{"ENT_TYPE": "DET"},
                    {"TEXT": "film"},
                    {"LOWER": {"IN": rs_string}}], 2))
    match_list.extend(
        match_film(doc, matcher,
                   [{"TEXT": "film"},
                    {"ENT_TYPE": "VERB"},
                    {"LOWER": {"IN": rs_string}}], 2))

    return match_list


def loadtranslatedtitles_imdb():
    tsv_filename = download_imdb_dataset(IMDB_URLS.TRANSLATION.value)

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    i = 0
    with gzip.open(tsv_filename, "rt") as csvfile:
        # with open(input_tsv_file, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        print("Inserting data...")
        for row in csvreader:
            if row["language"] == "FR":
                # Execute a SQL INSERT command
                sql = 'UPDATE move set TRANSLATED=%s where imdbid=%s'
                params = (row["title"], row["titleId"])
                cur.execute(sql, params)
                i = i + 1

        conn.commit()
    print("Ended")


def loadmovies_imdb():
    tsv_filename = download_imdb_dataset()

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    i = 0
    with gzip.open(tsv_filename, "rt") as csvfile:
    # with open(input_tsv_file, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        print("Inserting data...")
        for row in csvreader:
            if row["titleType"] == "movie":
                # Execute a SQL INSERT command
                sql = 'INSERT INTO movie VALUES (%s,%s,%s,%s)'
                params = (i, row["primaryTitle"], row["tconst"], row["primaryTitle"])
                cur.execute(sql, params)
                i = i + 1

        conn.commit()
    print("Ended")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--action", help="convert : chunk file, transcript : transcript into text the resul of "
                                         "conversion", required=True)
    parser.add_argument("--file", help="MP3 to transcript", required="--convert" in sys.argv)
    parser.add_argument("--chunkfolder", help="folder containing chunks", required="--transcript" in sys.argv)
    parser.add_argument("--transcriptedfile", help="file to parse", required="--parse" in sys.argv)
    parser.add_argument("--xmlfeedurl", help="Feed URL XML format", required="--download" in sys.argv)
    args = parser.parse_args()
    action = args.action
    sound_file_path = args.file
    chunk_folder = args.chunkfolder
    transcripted_file = args.transcriptedfile
    xml_feed_url = args.xmlfeedurl

    if args.action == "convert":
        conversion()

    if args.action == "transcript":
        transcription()

    if args.action == "parse":
        print(parse())

    if args.action == "download":
        download_rss_feed()

    if args.action == "loaddb":
        loadmovies_imdb()

    if args.action == "loaddbtranslated":
        loadtranslatedtitles_imdb()

    if args.action == "all":
        conversion()
        transcription(AUDIO_CHUNKS_FOLDER)
        parse(TEXT_FILE)
