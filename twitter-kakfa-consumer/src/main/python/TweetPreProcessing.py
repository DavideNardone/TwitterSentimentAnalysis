from HTMLParser import HTMLParser
from Emoticons import Emoticons
from Acronyms import Acronyms
from nltk.stem.porter import *
import sys

class TweetPreProcessing:

    # Replace Html character codes, replace emoticons with sentiment and replace acronym with words
    def preProcessing(self,text):

        # Html character codes (i.e., &alt; &amp;) are replaced with an ASCII equivalent.
        # FIXME: error with 'utf8' characters that can not be encoded...
        # work-around to fix 'utf-8' problem
        reload(sys)
        sys.setdefaultencoding('utf8')
        h = HTMLParser()
        text = h.unescape(text)

        # Due the conversion of text into 'unicode' in the last function,
        # text needs to be converted again into string format
        text = str(text)

        # Convert string to list
        text = text.split(" ")

        # REPLACE ALL THE 'EMOTICONS' WITH THERI SENTIMENT
        emoticons = Emoticons().getEmoticons()

        for em in emoticons:
            if em in text:
                ind = text.index(em)
                text[ind] = emoticons.get(em)

        # EXPANS ACRONYMS (USING AN ACRONYM DICTIONARY)
        acronyms = Acronyms().getAcronyms()

        text = [item.upper() for item in text]

        for acronym in acronyms:
            if acronym in text:
                ind = text.index(acronym)
                text[ind] = acronyms.get(acronym)

        text = [item.lower() for item in text]

        return ' '.join(text)

    # Remove punctuations, Symbols and number
    # all URLs(e.g.www.xyz.com), hash tags(e.g.  # topic), targets (@username)
    def tokenizing(self, text):
        text = text.lower().strip() #lower and remove space in text
        text = re.sub('(\d+)|(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', '', text)

        # Remove double space
        text = ' '.join(text.split())

        # remove white space (like trim)
        return text.strip()

    def stopping(self, text_list):

        # Reading stop words
        # TODO: Move into class
        stop_words = [];
        with open(
        '/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/resources/stopwords.txt') as f:
            for line in f:
                stop_words.append(line.replace("\n", ""))

        for ws in stop_words:
            if ws in text_list:
                text_list.remove(ws)

        return text_list

    # TODO: Remove Non - English Tweets
    # TODO: Correct the spellings; sequence of repeated characters need to be handled

    def stemming(self, text_list):

        stemmer = PorterStemmer()

        stems = [stemmer.stem(flex_word) for flex_word in text_list]

        #convert list utf8 to string
        stems_list = []
        for stem in stems:
            stems_list.append(stem.encode("utf-8"))

        return stems_list

    def processTweet(self, text):

        text = self.preProcessing(text)

        # TOKENIZING
        text = self.tokenizing(text).split(" ") #convert string to list


        # NOTE: Til now we've removed all duplicate (words and stop word),
        # but it has to be checked whether duplicate words can influence on the sentiment-weight of the world
        # (e.g. 'im', 'listening', 'by', 'danny', 'gokey', 'heart', 'heart', 'heart', 'aww', 'hes', 'so', 'amazing', 'i', 'heart', 'him', 'so', 'much', 'smile')

        # Remove duplicate in tweet preserving order (e.g. Fair enough. But i have the Kindle2 and i think it's perfect)
        # TODO: it should be done only for duplicate "stop word (maybe)"
        text_list = []
        for word in text:
            # replace two or more with two occurrences
            word = self.replaceTwoOrMore(word)

            # # check if the word stats with an alphabet
            # val = re.search(r"^[a-zA-Z][a-zA-Z0-9]*$", word)

            if word not in text_list:
                text_list.append(word)

        # STOPPING
        self.stopping(text_list)

        # STEMMING
        tweet_proc = self.stemming(text_list)

        return tweet_proc

    # replaceTwoOrMore words repetitions
    def replaceTwoOrMore(self, text):
        # look for 2 or more repetitions of character and replace with the character itself
        pattern = re.compile(r"(.)\1{1,}", re.DOTALL)
        return pattern.sub(r"\1\1", text)

    def getPolarity(self, text):

        polarity = text[0]

        return polarity

    def getId(self, text):

        id = int(text[1])

        return id

    def getDate(self, text):

        date = text[2]

        return date

    def getQuery(self, text):

        query = text[3]

        return query

    def getSender(self, text):

        sender = text[4]

        return sender

    def getTweet(self, text):

            tweet = text[5]

            return tweet

    def TweetBuilder(self, text):

        ############ FOR TRAINING ############
        # for word in text:
        #         loc = text.index(word)
        #         text[loc] = word.replace('"','')
        ############ FOR TRAINING ############

        polarity = self.getPolarity(text)
        tweet = self.getTweet(text)

        tweet_stemmed = self.processTweet(tweet)

        tweets = []
        tweets.append((polarity, tweet_stemmed))

        return tweets