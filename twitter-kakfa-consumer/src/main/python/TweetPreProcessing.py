import re
import os
import json
from Emoticons import Emoticons
from Acronyms import Acronyms

class TweetPreProcessing:

    # sentiment = ''

    # def __init__(self):

    def stemming(self, text):

        # make all word in the string lower
        text = text.lower()

        # convert string to list
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

        # Remove duplicate in tweet (e.g. Fair enough. But i have the Kindle2 and i think it's perfect)
        #TODO: it should be done only for duplicate "stop word"
        text_list = []
        for word in text:
            if word not in text_list:
                text_list.append(word)

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


    def cleanUpTweet(self, text):
        # Remove punctuations, Symbols and number
        # all URLs(e.g.www.xyz.com), hash tags(e.g.  # topic), targets (@username)

        text = text.lower().strip() #lower and remove space in text
        # text = re.sub("\d+", '', text)  # Remove digit
        text = re.sub('(\d+)|(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', '', text)

        # Remove double space
        text = ' '.join(text.split())

        # remove withe space (like trim)
        return text.strip()


    def getPolarity(self, text):
        polarity = text[0]

        return polarity

    def getId(self, text):
        id = text[1]

        return id

    def getDate(self, text):
        date = text[2]

        return date

    def getReceiver(self, text):
        receiver = text[3]

        return receiver

    def getSender(self, text):
        sender = text[4]

        return sender


    def getTweet(self, text):
            tweet = text[5]

            return tweet

    def TweetBuilder(self, text):

        # text = yaml.load(text)
        text = json.loads(text.decode('ascii'))

        # turn unicode list elements into string element
        for word in text:
            loc = text.index(word)
            text[loc] = str(word)

        polarity = self.getPolarity(text)
        # print(polarity)
        # id = self.getId(text)
        # print(id)
        # date = self.getDate(text)
        # print(date)
        # receiver = self.getReceiver(text)
        # print(receiver)
        # sender = self.getSender(text)
        # print(sender)
        tweet = self.getTweet(text)
        # print(tweet)

        tweet_stemmed = self.stemming(tweet)

        tweet_stemmed.insert(0,polarity)

        tweet_stemmed = ' '.join(tweet_stemmed)
        # print(tweet_stemmed)
        return tweet_stemmed