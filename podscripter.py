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
TEXT_FILE = "transcribed.txt"
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


def download_rss_feed():
    """Download MP3 of an audio podcast coming from RSS feed"""
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


def chunk_wav_file(wav_file, folder=AUDIO_CHUNKS_FOLDER):
    """Splitting the large audio file into chunks"""
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
        __progress(i, len(chunks), "Writing chunks")
        chunk_filename = os.path.join(folder, f"chunk{i:04}.wav")
        audio_chunk.export(chunk_filename, format="wav")

    print("Ended")

    return audio_chunk


def __vosk_capture(model, recorder, audiofile_path):
    """Captures sound and convert it to text"""
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

    return json.loads(recorder.FinalResult())["text"]


def __speed_change(audiofile, speed=1.0):
    """Change the speed of an audio file to improve voice capture by VOSK"""
    # Manually override the frame_rate. This tells the computer how many
    # samples to play per second
    sound_with_altered_frame_rate = audiofile._spawn(audiofile.raw_data, overrides={
        "frame_rate": int(audiofile.frame_rate * speed)
    })

    # convert the sound with altered frame rate to a standard frame rate
    # so that regular playback programs will work right. They often only
    # know how to play audio at standard frame rate (like 44.1k)
    return sound_with_altered_frame_rate.set_frame_rate(audiofile.frame_rate)


def __sound_convert_to_wav(mp3_filepath):
    """converts MP3 to WAV (needed by VOSK)"""
    filename, file_extension = os.path.splitext(mp3_filepath)
    # convert mp3 to wav
    sound = AudioSegment.from_mp3(mp3_filepath)
    # sound = speed_change(sound, 0.5)
    wav_file_path = filename + ".wav"
    sound.export(wav_file_path, format="wav")

    return wav_file_path


def __write_line(text_to_write, name_of_file=TEXT_FILE):
    """ Write a line in a file"""
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


def __progress(count, total, status=''):
    """Displays a progress bar"""
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


def transcription():
    """ Transcribe WAVE audio file into text using VOSK library
    Produce a text file"""
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
        __progress(i, len(list_of_chunks), "Transcribing")
        text_transcribed = __vosk_capture(model, rec, chunk_folder + '/' + chunk_filename)
        if text_transcribed != "":
            __write_line(text_transcribed, filename)

    print("\nEnded")


def conversion():
    """Convert MP3 to WAV file and split on silence"""
    chunks = []
    wav_file = __sound_convert_to_wav(sound_file_path)
    folder = os.path.splitext(sound_file_path)[0]
    chunks = chunk_wav_file(wav_file, folder)


def __database_extraction():
    """Extract all movie title from PL/SQL DB and return a result set
    We only get films with ratings to avoid being polluted by films with limited diffusion
    We get the films ordered by length of title desc, for a better match with the transcribed file"""
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
    """ Fine match a film with a specific pattern. Returns the list of matches. The position
    is the part of the match we want to find so the pattern should not contain any optional criterion
    (spacy can't tell which part of the pattern corresponds to the match)"""
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


def __brute_match(rs, transcribed_text):
    """Brute match the IMDB DB with the transcribed text, to detect film title only with a classic regexp
    This leads to a lot a false positives but still filters the list for the fine-grained further
    spacy matching process"""
    rs_string = []

    # setting up exception list, we have the word "film" to avoid the matcher to consider it as a film title since it
    # would make it miss matching rule MR1/2/3 (MR containg the word film)
    exception_list = []
    exception_list.append("film")

    # using case folded transcribed text to match without case consideration
    transcribed_text_casefolded = transcribed_text.casefold()

    # going through the list of title to make a brute match first
    for title in rs[0]:
        text_title = title[0]
        text_title_casefold = text_title.casefold()
        # we avoid getting film with less than 2 characters, that would make too much false positives
        # we convert to brute-detected film name with underscore to make it work with spacy matcher (if we have a space
        # in the title, spacy will consider it as a new token and miss a match)
        if len(text_title) > 2 \
                and text_title_casefold in transcribed_text_casefolded \
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
            transcribed_text = re.sub(r"\b%s\b" % text_title, replaced_title, transcribed_text, flags=re.IGNORECASE)

    return rs_string, transcribed_text


def __fine_match(rs_string, transcribed_text):
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
    doc = nlp(transcribed_text)
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
    with open(transcribed_file) as f:
        lines = f.readlines()
    f.close()
    transcribed_text = ' '.join(lines)

    # loading data from DB
    rs = __database_extraction()

    # proceed with brute match
    rs_string, transcribed_text = __brute_match(rs, transcribed_text)

    # proceed with spacy fine match
    return __fine_match(rs_string, transcribed_text)


def loadtranslatedtitles_imdb():
    """Downloads and loads translated movie titles from IMDB downloaded dataset"""
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
    """Downloads and loads movie ratings from IMDB downloaded dataset"""
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
    """Downloads and loads movie from IMDB downloaded dataset"""
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
    """ Prepare the transcribed file to be analyzed by SPACY matcher and overwrite it.
    The only action made for now is to capitalize names and surnames based on philipperemy name-dataset
    This action is here to provide SPACY matcher a file where names and surnames can be identified and not
    mingled with adjectives.
    Uses the top 500 first-names in FR, GB and US. The other nationalities would not be correctly recognized
    and transcribed by VOSK anyway"""
    nd = NameDataset()

    with open(transcribed_file, "r") as f:
        lines = f.readlines()
    f.close()

    transcribed_text = ' '.join(lines)

    nlp = spacy.load('/home/sylvain/.local/lib/python3.8/site-packages/fr_core_news_md/fr_core_news_md-3.2.0')
    doc = nlp(transcribed_text)
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
        transcribed_text = re.sub(r"\b%s\b" % name, name.capitalize(), transcribed_text)

    with open(transcribed_file, "w") as f:
        f.truncate(0)
        f.write(transcribed_text)
    f.close()

    print("End")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--action", help="convert : chunk file, transcript : transcript into text the resul of "
                                         "conversion", required=True)
    parser.add_argument("--file", help="MP3 to transcript", required="--convert" in sys.argv)
    parser.add_argument("--chunkfolder", help="folder containing chunks", required="--transcript" in sys.argv)
    parser.add_argument("--transcribedfile", help="file to pre-parse",
                        required="--preparse" in sys.argv or "--parse" in sys.argv)
    parser.add_argument("--xmlfeedurl", help="Feed URL XML format", required="--download" in sys.argv)
    args = parser.parse_args()
    action = args.action
    sound_file_path = args.file
    chunk_folder = args.chunkfolder
    transcribed_file = args.transcribedfile
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
