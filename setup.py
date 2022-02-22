from setuptools import setup

setup(
    name='podscripter',
    version='0.1.0',
    description='Podcast analyzer',
    url='https://github.com/erikltt/podscripter/',
    author='Sylvain Kittler',
    author_email='erikltt@hotmail.com',
    license='None',
    packages=['podscripter'],
    install_requires=['spacy', 'requests', 'feedparser', 'pydub', 'vosk'],
    classifiers=[]
)
