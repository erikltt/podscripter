#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import os
import pathlib
import re
import subprocess
import sys
from enum import Enum
from os import listdir
from os.path import isfile, join
from urllib.parse import urlparse

import feedparser
import psycopg2 as psycopg2
import requests
import spacy as spacy
from pydub import AudioSegment
from pydub.silence import split_on_silence
from spacy.matcher import Matcher
from vosk import Model, KaldiRecognizer, SetLogLevel
from names_dataset import NameDataset

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
                              ("TRANSLATION", "https://datasets.imdbws.com/title.akas.tsv.gz"),
                              ("RATING", "https://datasets.imdbws.com/title.ratings.tsv.gz")])


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
    sql = 'select translated, imdbid from movie where rating > 0 order by length(translated) desc'
    cur.execute(sql)
    rs.append(cur.fetchall())

    return rs


def __match_film(doc, matcher, pattern, position):
    match_list = []
    match_text = []
    matcher.add("FILM", [pattern])
    matches = matcher(doc)
    matcher.remove("FILM")

    for match_id, start, end in matches:
        match_list.append(str.replace(doc[start + position].text, '_', ' '))
        match_text.append(str.replace(doc[start:end].text, '_', ' '))

    print("Pattern selected: ", match_list)
    print("Matched text    : ", match_text)

    return match_list


def __brute_match(rs, transcripted_text):
    """Brute match the IMDB DB with the transcripted text, to detect film title only with a classic regexp
    This leads to a lot a false positives but still filters the list for the fine-grained further
    spacy matching process"""
    rs_string = []

    # setting up exception list, we have the word "film" to avoid the matcher to consider it as a film title since it
    # would make it miss matching rule MR1/2/3 (MR containg the word film)
    exception_list = []
    exception_list.append("film")

    # using case folded transcripted text to match without case consideration
    transcripted_text_casefolded = transcripted_text.casefold()

    # going through the list of title to make a brute match first
    for title in rs[0]:
        text_title = title[0]
        text_title_casefold = text_title.casefold()
        # we avoid getting film with less than 2 characters, that would make too much false positives
        # we convert to brute-detected film name with underscore to make it work with spacy matcher (if we have a space
        # in the title, spacy will consider it as a new token and miss a match)
        if len(text_title) > 2 \
                and text_title_casefold in transcripted_text_casefolded \
                and text_title_casefold not in exception_list:
            # we avoid modifying film title containing only one word, it could be a name or surname, and that would
            # interfere with the preparse process that uppercase such words to make spacy recognize them as "PROPN"
            if len(text_title.split()) > 1:
                replaced_title = str.replace(text_title, ' ', '_').lower()
            else:
                replaced_title = text_title
            # we add the brute-matched title to the list of possible real match
            rs_string.append(replaced_title)
            # rewrite the text with this possible match (with _ instead of spaces)
            transcripted_text = re.sub(r"\b%s\b" % text_title, replaced_title, transcripted_text, flags=re.IGNORECASE)

    return rs_string, transcripted_text


def __fine_match(rs_string, transcripted_text):
    """Fine-matching using spacy matcher and the following matching rules
    MR1 :   {"LOWER": "film"}, {"LOWER": {"IN": rs_string}}, {"POS": "PROPN"}
        --> film vous ne désirez que moi Claire Simon
    MR2 :   {"POS": "DET"}, {"LOWER": "film"}, {"LOWER": {"IN": rs_string}}, {"TEXT": "de"}
        --> Le film les jeunes amants de carine tardieu
    MR3 :   {"POS": "ADJ"}, {"LOWER": {"IN": rs_string}}, {"POS": "DET", "OP": "?"}, {"POS": "NOUN"},
            {"POS": "ADP", "OP": "?"}, {"LOWER": "film"}
        -->  réjouissant les voisins de mes voisins sont mes voisins (un) drôle (de) film
    MR4 :   {"LEMMA": "voir"}, {"POS": "DET"}, {"LOWER": {"IN": rs_string}}, {"POS": "VERB", "OP": "!"}
        --> voir ce petite solange
    MR5 :   {"LEMMA": "voir"}, {"LOWER": {"IN": rs_string}}, {"POS": "VERB", "OP": "!"}
        --> voir teresa la voleuse
    """
    nlp = spacy.load('/home/sylvain/.local/lib/python3.8/site-packages/fr_core_news_md/fr_core_news_md-3.2.0')

    matcher = Matcher(nlp.vocab)
    doc = nlp(transcripted_text)
    match_list = []

    # MR1 : film vous ne désirez que moi claire simon
    match_list.extend(
        __match_film(doc, matcher,
                   [{"LOWER": "film"},
                    {"LOWER": {"IN": rs_string}},
                    {"POS": "PROPN"}], 1))
    # MR2 : Le film les jeunes amants de carine tardieu
    match_list.extend(
        __match_film(doc, matcher,
                   [{"POS": "DET"},
                    {"LOWER": "film"},
                    {"LOWER": {"IN": rs_string}},
                    {"TEXT": "de"}], 2))
    # MR3 : réjouissant les voisins de mes voisins sont mes voisins (un) drôle (de) film
    match_list.extend(
        __match_film(doc, matcher,
                   [{"POS": "ADJ"},
                    {"LOWER": {"IN": rs_string}},
                    {"POS": "DET", "OP": "?"},
                    {"POS": "NOUN"},
                    {"POS": "ADP", "OP": "?"},
                    {"LOWER": "film"}], 1))
    # MR4 : voir ce petite solange
    match_list.extend(
        __match_film(doc, matcher,
                   [{"LEMMA": "voir"},
                    {"POS": "DET"},
                    {"LOWER": {"IN": rs_string}},
                    {"POS": "VERB", "OP": "!"}], 2))
    # MR5 : voir teresa la voleuse
    match_list.extend(
        __match_film(doc, matcher,
                   [{"LEMMA": "voir"},
                    {"LOWER": {"IN": rs_string}},
                    {"POS": "VERB", "OP": "!"}], 1))
    # match_list.extend(
    #     match_film(doc, matcher,
    #                [{"ENT_TYPE": "DET"},
    #                 {"TEXT": "film"},
    #                 {"LOWER": {"IN": rs_string}}], 2))
    # match_list.extend(
    #     match_film(doc, matcher,
    #                [{"TEXT": "film"},
    #                 {"ENT_TYPE": "VERB"},
    #                 {"LOWER": {"IN": rs_string}}], 2))
    # match_list.extend(
    #     match_film(doc, matcher,
    #                [{"POS": "ADJ"},
    #                 {"LOWER": {"IN": rs_string}},
    #                 {"POS": "ADP"}], 1))
    #
    # # FILM avec Gérard Depardieu (FILM + nom propre)
    # match_list.extend(
    #     match_film(doc, matcher,
    #                [{"LOWER": {"IN": rs_string}},
    #                 {"TEXT": "avec"},
    #                 {"ENT_TYPE": "PER"}], 0))
    #
    # # FILM formidable (film + adjectif)
    # match_list.extend(
    #     match_film(doc, matcher,
    #                [{"LOWER": {"IN": rs_string}},
    #                 {"ENT_TYPE": "ADJ"},
    #                 ], 0))
    return list(dict.fromkeys(match_list))


def parse():
    """Parse the input file to match film contained in the text.
    1. Load the file
    2. Extract the IMDB DB
    3. Brute match with simple regex
    4. Fine match with SPACY matcher"""
    # loading data from input text file
    with open(transcripted_file) as f:
        lines = f.readlines()
    f.close()
    transcripted_text = ' '.join(lines)

    # loading data from DB
    rs = database_extraction()

    # proceed with brute match
    rs_string, transcripted_text = __brute_match(rs, transcripted_text)

    # proceed with spacy fine match
    return __fine_match(rs_string, transcripted_text)


def loadtranslatedtitles_imdb():
    tsv_filename = download_imdb_dataset(IMDB_URLS.TRANSLATION.value)

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    i = 0
    with gzip.open(tsv_filename, "rt") as csvfile:
        # with open(input_tsv_file, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Updating data...")
        for row in csvreader:
            # if row["region"] == "FR" and row["language"] == "fr":
            if row["region"] == "FR":
                # Execute a SQL INSERT command
                sql = 'UPDATE movie set TRANSLATED=%s where imdbid=%s'
                params = (row["title"], row["titleId"])
                cur.execute(sql, params)
                i = i + 1

        conn.commit()
    print("Ended")


def loadrating_imdb():
    tsv_filename = download_imdb_dataset(IMDB_URLS.RATING.value)

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    i = 0
    with gzip.open(tsv_filename, "rt") as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Updating data...")
        for row in csvreader:
            # Execute a SQL UPDATE command
            sql = 'UPDATE movie set rating=%s where imdbid=%s'
            params = (row["averageRating"], row["tconst"])
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
                params = (i, row["originalTitle"], row["tconst"], row["originalTitle"])
                cur.execute(sql, params)
                i = i + 1

        conn.commit()
    print("Ended")


def preparse():
    nd = NameDataset()

    with open(transcripted_file, "r") as f:
        lines = f.readlines()
    f.close()

    transcripted_text = ' '.join(lines)

    nlp = spacy.load('/home/sylvain/.local/lib/python3.8/site-packages/fr_core_news_md/fr_core_news_md-3.2.0')
    doc = nlp(transcripted_text)
    # Create list of word tokens after removing stopwords

    token_list = []
    common_names = []
    common_names_dict = nd.get_top_names(n=500, use_first_names=True, country_alpha2='FR')
    common_names.extend(common_names_dict["FR"]["M"])
    common_names.extend(common_names_dict["FR"]["F"])
    common_names_dict = nd.get_top_names(n=500, use_first_names=True, country_alpha2='US')
    common_names.extend(common_names_dict["US"]["M"])
    common_names.extend(common_names_dict["US"]["F"])
    common_names_dict = nd.get_top_names(n=500, use_first_names=True, country_alpha2='GB')
    common_names.extend(common_names_dict["GB"]["M"])
    common_names.extend(common_names_dict["GB"]["F"])

    # common_names = [name.lower() for name in common_names]

    for token in doc:
        if not token.is_stop:  # and token.pos_ == "NOUN":
            if token.text.capitalize() in common_names:
                token_list.append(token.text)

    token_list = list(dict.fromkeys(token_list))

    for name in token_list:
        transcripted_text = re.sub(r"\b%s\b" % name, name.capitalize(), transcripted_text)

    with open(transcripted_file, "w") as f:
        f.truncate(0)
        f.write(transcripted_text)
    f.close()

    print("End")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--action", help="convert : chunk file, transcript : transcript into text the resul of "
                                         "conversion", required=True)
    parser.add_argument("--file", help="MP3 to transcript", required="--convert" in sys.argv)
    parser.add_argument("--chunkfolder", help="folder containing chunks", required="--transcript" in sys.argv)
    parser.add_argument("--transcriptedfile", help="file to pre-parse",
                        required="--preparse" in sys.argv or "--parse" in sys.argv)
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

    if args.action == "preparse":
        print(preparse())

    if args.action == "parse":
        print(parse())

    if args.action == "download":
        download_rss_feed()

    if args.action == "loaddb":
        loadmovies_imdb()

    if args.action == "loadrating":
        loadrating_imdb()

    if args.action == "loaddbtranslated":
        loadtranslatedtitles_imdb()
