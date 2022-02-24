# podscripter
Extract information from podcasts. When listening to podcasts, I often hear people talk about great movies they saw and they want to share.  
The idea is to get this information out of these audio files. It is not meant to be comprehensive.  
It is based on the use of Spacy NLP programming, VOSK speech recognition, with the help of IMDB datasets, and name-dataset from  https://github.com/philipperemy/name-dataset  
Currently only works for movies, and french podcasts.
## Installation
### git repository
`git clone https://github.com/erikltt/podscripter`
### VOSK installation
Install VOSK and copy french model to your local disk inside "model" folder  
`pip3 install vosk`  
`wget https://alphacephei.com/vosk/models/vosk-model-fr-0.6-linto.zip`  
`unzip vosk-model-small-en-us-0.15.zip`  
`mv vosk-model-small-en-us-0.15 model` 
### SPACY installation
Install spacy and french model  
`pip install -U spacy`  
`python -m spacy download fr_core_news_md`
### Name-dataset installation
Name-dataset is a python library containing list of common names used in this project  
`pip install names-dataset`
## Initialisation
The following actions are not mandatory as the script comes with a preloaded DB containing IMDB datasets updated the 22nd of Februar 2022.  
These actions are long as the dataset from IMDB is rather big (~9M lines)
### Init/refresh of movie DB from IMDB datasets
`python init.py --action initdb`
### Load IMDB movie dataset
`python init.py --action imdbmovie`
### Load IMDB translated titles
`python init.py --action imdbtranslate`
### Load IMDB movie ratings
`python init.py --action imdbratings`
## Usage
The process is subdivided into several steps  
1. Convert the podcast into small audio chunks
2. Transcribe the chunks into text
3. Preparse the file
4. Parse the file  
### Commands
`python podscripter.py --action convert --file 14007-02.01.2022-ITEMA_22886172-2022F4007S0002-22.mp3`  
`python podscripter.py --action transcribe --chunkfolder audio-chunk`  
`python podscripter.py --action preparse --transcribedfile 14007-02.01.2022-ITEMA_22886172-2022F4007S0002-22.txt`  
`python podscripter.py --action parse --transcribedfile 14007-02.01.2022-ITEMA_22886172-2022F4007S0002-22.txt`  
### Result  
`Pattern selected:  ['vous ne désirez que moi']`  
`Matched text    :  ['film vous ne désirez que moi Claire']`  
`Pattern selected:  ['les jeunes amants']`  
`Matched text    :  ['le film les jeunes amants de']`  
`Pattern selected:  ['les voisins de mes voisins sont mes voisins']`  
`Matched text    :  ['réjouissant les voisins de mes voisins sont mes voisins un drôle de film']`  
`Pattern selected:  ['petite solange']`  
`Matched text    :  ["voir ce petite solange d'"]`  
`Pattern selected:  ['teresa la voleuse']`  
`Matched text    :  ['voir teresa la voleuse moi']`  
`['vous ne désirez que moi', 'les jeunes amants', 'les voisins de mes voisins sont mes voisins', 'petite solange', 'teresa la voleuse']`
