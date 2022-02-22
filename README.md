# podscripter
Extract informations from podcasts
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
### PL/SQL DB installation
Install psycopg2 to use PL/SQL DB
`pip install psycopg2`
