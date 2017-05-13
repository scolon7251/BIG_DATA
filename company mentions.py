import json
import yahoo_finance as yf
import csv
import glob

json_list = glob.glob('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/*/*/*.json.bz2')
RDD = sc.textFile(','.join(json_list))


reader=csv.DictReader(open('/Users/jonathandriscoll/PycharmProjects/ProjectC/posneg.csv'))
words=[]
for row in reader:
    words.append(row)




def mention_it(list):
    kw_list = ['microsoft', 'Microsoft' 'msft', 'ibm', 'starbucks', 'sbux', 'nvidia', 'nvda','yahoo', 'yhoo','trump','Trump']
    for string in kw_list:
        if string in str(list).split(' '):
            return '1'
        else:
            return '0'

def company_tweets(list):
    tweet_list=[]
    if mention_it(list)=='1':
        tweet_list.append(list)
    return tweet_list



"""returns a list of all tokenized words that appear in dictionary, to be used in scorekeeper"""
def in_words(token_list):
    list=[]
    for word in words:
        if word['Words'].lower() in token_list:
            list.append.(word).lower()
    return list


"""scorekeeper takes words that are present in sentiment_dict, and tallies them.  +1 for positive, -1 for negative, +0 for neutral.  Returns final score"""


def scorekeeper(word):
    if word['Positive'] != '0':
        score=1
    else:
        score = -1
    return score

"""applies scorekeeper to each tweet and returns a list of sentiment scores."""
def get_sentiment(tweet):
    score=0
    for word in tweet:



def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices



