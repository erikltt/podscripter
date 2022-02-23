# podscripter
Extract information from podcasts. When listening to podcasts, I often hear people talk about great films they saw and they want to share. The idea is to get this information out of these audio files. It is not meant to be comprehensive. It is based on the use of Spacy NLP programming, with the help of IMDB datasets, and name-dataset from  https://github.com/philipperemy/name-dataset
## Installation
### git repository
`git clone https://github.com/erikltt/podscripter`
### VOSK installation
Install VOSK and copy french model to your local disk inside "model" folder  
`pip3 install vosk`  
`wget https://alphacephei.com/vosk/models/vosk-model-fr-0.6-linto.zip`  
`unzip vosk-model-small-en-us-0.15.zip`  
`mv vosk-model-small-en-us-0.15 model`  
### Name-dataset installation
Name-dataset is a python library containing list of common names used in this project  
`pip install names-dataset`
### TO BE CONTINUED
