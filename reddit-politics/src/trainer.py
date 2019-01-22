import os.path
import logging

from gensim import corpora, models, similarities

import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

import newspaper

import pymysql
from pymysql import cursors

from . import config


class Trainer(object):
    def __init__(self):
        self.nconfig = newspaper.Config()
        self.nconfig.MIN_WORD_COUNT = 150
        self.nconfig.memoize_articles = False
        self.nconfig.fetch_images = False

    def load_training_data(self, from_sql=True, custom_data=None):
        if not from_sql:
            if custom_data is None:
                print('Error: Must input data if not from SQL')
                return
            self._clean_and_save(custom_data)
            return

        db = pymysql.connect(**config.mysql)
        cursor = db.cursor(cursors.DictCursor)

        data_query = 'SELECT * FROM {}'.format(config.submission_table_name)

        cursor.execute(data_query)
        data = [self._process_article(submission['url']) for submission in cursor.fetchall()]
        self._clean_and_save(data)

    def train(self):
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

        if os.path.exists(config.DICTIONARY_PATH):
            dictionary = corpora.Dictionary.load(config.DICTIONARY_PATH)
            corpus = corpora.MmCorpus(config.CORPUS_PATH)
        else:
            print('Error: No data exists yet')
            return

        tfidf = models.TfidfModel(corpus)
        corpus_tfidf = tfidf[corpus]

        # CHOOSE WHAT TYPE OF MODEL TO USE!!
        lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=10)
        lsi.save(config.LSI_MODEL_PATH)

        index = similarities.MatrixSimilarity(lsi[corpus], num_features=len(dictionary))
        index.save(config.INDEX_PATH)

    def _process_article(self, url):
        article = newspaper.Article(url=url, config=self.nconfig)
        article.download()
        article.parse()
        return article.text.encode('utf-8')

    def _load_stopwords(self):
        return stopwords.words('english')

    def _get_wordnet_pos(self, word):
        """ Returns the NLTK WordNet part-of-speech object for a given word. If
            the word is unknown, this method returns wordnet.NOUN.

        Args
            word (string): word to find part-of-speech of

        Returns
            wordnet part-of-speech: one of wordnet.ADJ, wordnet.NOUN, wordnet.VERB,
            or wordnet.ADV
        """

        tag = nltk.pos_tag([word])[0][1][0].upper()
        tag_dict = {"J": wordnet.ADJ,
                    "N": wordnet.NOUN,
                    "V": wordnet.VERB,
                    "R": wordnet.ADV}
        return tag_dict.get(tag, wordnet.NOUN)

    def _clean_and_save(self, data):
        """ Takes a list of documents, removes stopwords, tokenizes and lemmatizes,
            and saves it as a gensim Dictionary and MmCorpus in the path specified
            in the config file.

        Args
            data (list of strings): list of UTF-8 documents to be trained on

        Returns
            processed_data (list of list of strings): each element represents one
            document, which has been converted to a list of cleaned tokens
        """

        stop_words = self._load_stopwords()
        tokenizer = RegexpTokenizer(r'\w+')
        lemmatizer = WordNetLemmatizer()

        processed_data = [[lemmatizer.lemmatize(word, self._get_wordnet_pos(word))
                           for word in tokenizer.tokenize(d.lower())
                           if word not in stop_words]
                           for d in data]

        dictionary = corpora.Dictionary(processed_data)
        corpus = [dictionary.doc2bow(d) for d in processed_data]

        dictionary.save(config.DICTIONARY_PATH)
        corpora.MmCorpus.serialize(config.CORPUS_PATH, corpus)

        return processed_data
