import re
import os
import sys
import json
from HTMLParser import HTMLParser
from Emoticons import Emoticons
from Acronyms import Acronyms
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

class TweetPreProcessing:

    # sentiment = ''

    # def __init__(self):

    def processTweet(self, text):

        #Convert to lower case
        text = text.lower()

        # Html character codes (i.e., &alt; &amp;) are replaced with an ASCII equivalent.
        # FIXME: probably error with 'utf8' characters whose are not encoded...
        # work-around to fix 'utf-8' problem
        reload(sys)
        sys.setdefaultencoding('utf8')
        h = HTMLParser()
        text = h.unescape(text)

        # Due the conversion of text into 'unicode' in the last function,
        # text needs to be converted again into string
        text = str(text)

        # Convert string to list
        text = text.split(" ")

        # Replace all the emoticons with their sentiment.
        emoticons = Emoticons().getEmoticons()

        for word in text:
            if word in emoticons:
                loc = text.index(word)
                text[loc] = emoticons.get(word)

        # Convert list to string
        text = ' '.join(text)

        #Cleaning up string
        text = self.cleanUpTweet(text).split(" ") #then .split(" ") convert string to list

        # Create stop words
        # TODO: Move into class
        stop_words = [];
        with open(os.getcwd()+'/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/resources/stopwords.txt') as f:
            for line in f:
                stop_words.append(line.replace("\n", ""))

        # NOTE: Til now we've removed all duplicate (words and stop word),
        # but it has to be checked whether duplicate words can influence on the sentiment-weight of the world
        # (e.g. 'im', 'listening', 'by', 'danny', 'gokey', 'heart', 'heart', 'heart', 'aww', 'hes', 'so', 'amazing', 'i', 'heart', 'him', 'so', 'much', 'smile')

        # Remove duplicate in tweet preserving order (e.g. Fair enough. But i have the Kindle2 and i think it's perfect)
        # TODO: it should be done only for duplicate "stop word"
        text_list = []
        for word in text:
            # replace two or more with two occurrences
            word = self.replaceTwoOrMore(word)

            # # check if the word stats with an alphabet
            # val = re.search(r"^[a-zA-Z][a-zA-Z0-9]*$", word)

            if word not in text_list:
                text_list.append(word)

        #ignore if it is a stop word
        for ws in stop_words:
            if ws in text_list:
                text_list.remove(ws)

        # Expand Acronyms(using an acronym dictionary)
        acronyms = Acronyms().getAcronyms()

        for word in text_list:
            upper_word = str(word).upper()
            if upper_word in acronyms:
                loc = text_list.index(word)
                text_list[loc] = acronyms.get(upper_word)

        # TODO: Correct the spellings; sequence of repeated characters is to be handled
        # TODO: Remove Non - English Tweets

        return text_list


    # Remove punctuations, Symbols and number
    # all URLs(e.g.www.xyz.com), hash tags(e.g.  # topic), targets (@username)
    def cleanUpTweet(self, text):
        text = text.lower().strip() #lower and remove space in text
        # text = re.sub("\d+", '', text)  # Remove digit
        text = re.sub('(\d+)|(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', '', text)

        # Remove double space
        text = ' '.join(text.split())

        # remove withe space (like trim)
        return text.strip()

    # start replaceTwoOrMore
    def replaceTwoOrMore(self, text):
        # look for 2 or more repetitions of character and replace with the character itself
        pattern = re.compile(r"(.)\1{1,}", re.DOTALL)
        return pattern.sub(r"\1\1", text)


    def getPolarity(self, text):

        # if text[0] in '0':
        #     polarity = 'negative'
        # elif text[0] in '2':
        #     polarity = 'neutral'
        # else:
        #     polarity = 'positive'

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


    def ProcessTrainingTweets(self,text):

        # text = json.loads(text.decode('ascii'))
        ############ FOR TRAINING ############
        for word in text:
            loc = text.index(word)
            text[loc] = word.replace('"','')
        ############ FOR TRAINING ############

        polarity = self.getPolarity(text)
        # print(polarity)
        id = self.getId(text)
        # print(id)
        # date = self.getDate(text)
        # print(date)
        # query = self.getQuery(text)
        # print(query)
        # sender = self.getSender(text)
        # print(sender)
        tweet = self.getTweet(text)
        # print(tweet)

        tweet_stemmed = self.processTweet(tweet)

        # tweets = (tweet_stemmed,polarity)

        # tweets = tweet_stemmed + polarity

        # print(type(tweets))
        # print(tweets)

        tweets = []
        tweets.append((polarity, tweet_stemmed))

        return tweets



    def TweetBuilder(self, text):

        ############ FOR TRAINING ############
        # for word in text:
        #         loc = text.index(word)
        #         text[loc] = word.replace('"','')
        ############ FOR TRAINING ############

        ############ STREAMING ############
        text = json.loads(text.decode('ascii'))

        # turn unicode list elements into string element
        for word in text:
            loc = text.index(word)
            text[loc] = str(word)
        ############ STREAMING ############

        polarity = self.getPolarity(text)
        # print(polarity)
        id = self.getId(text)
        # print(id)
        # date = self.getDate(text)
        # print(date)
        # query = self.getQuery(text)
        # print(query)
        # sender = self.getSender(text)
        # print(sender)
        tweet = self.getTweet(text)
        # print(tweet)

        tweet_stemmed = self.processTweet(tweet)


        # tweets = (tweet_stemmed,polarity)

        # tweets = tweet_stemmed + polarity

        # print(type(tweets))
        # print(tweets)

        tweets = []
        tweets.append((polarity, tweet_stemmed))


        return tweets