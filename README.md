This is a collection of Python 3 tools to ingest text from various sources (RSS feeds, text files, etc.), build Markov chains, and generate text.

Usage
=====
Install requisites using `pip3 install -r requirements.txt` in your virtualenv.
Then, use one of the scripts to load data from a source (text files, Twitter, RSS feeds, mail dumps) and produce an avro utterance file. This file contains the texts from the source and, when possible, metadata such as the timestamp and tags (for example, in case of a mailbox each mail is tagged with the from and to addresses).
 
 Now that avro files are ready, you need Redis. If you have Docker, you can use `docker run -p 6379:6379 redis`.
  
 Then use `markov_from_avro.py` to build a model and generate text based on it. Use `-h`  on any script to have a description of its parameters. You can store multiple models on the same Redis instance varying the _prefix_ parameter.
 
 
 __N-gram size__ you can specify the length of N-grams with the keylen parameter. Note that you have to use the same keylength to build and to generate the model.
 
 __tokenizer__ you can split the text by words or by characters, see the _tokenize_ parameter
 
 __mix models__ the build function doesn't empty Redis when starting, so you can invoke it on multiple avro files and mix the models. The resulting chain will mix N-grams from the various given sources. You can use tags to narrow models to a specific kind of text (e.g. hashtags on Twitter or senders for mailboxes)

 
Sources
=======
* RSS feeds
* Twitter
* text files in a folder
* mail dumps (Gmail by using google takeout, other mail clients)
* HackerNews comments
* chat logs from Adium (IRC, Telegram)
* Wikiquote dump
