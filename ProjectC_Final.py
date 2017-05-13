from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import Tokenizer
sc=SparkContext()
sq=SQLContext(sc)
from collections import Counter
import numpy as np
import glob
import yahoo_finance as yf
from yahoo_finance import Share
from pprint import pprint
import pandas_datareader.data as web
import os
import sys

"""This strategy will be tracking five stocks:  Starbucks, Microsoft, IBM, Nvidia, and Yahoo"""

##For standalone program
APP_NAME = "November Rain App"

##get working directory
#working_dir = os.getcwd()
root_dir = sys.argv[1]  # argv[1] is a command line argument that represents the location of the tarball directory
sentiment_dict = sys.argv[2]  # argv[2] is the location of posneg.json
folder_list = glob.glob(root_dir)  # lists all files in root directory

kw_list = ['Microsoft', 'MSFT', 'IBM', 'SBUX', 'Starbucks', 'NVDA', 'Nvidia', 'YHOO', 'Yahoo']  # list of keywords.

"""opens the sentiment dictionary and stores it as posneg"""
with open(sentiment_dict,'r') as infile:
    posneg=json.load(infile)

"""Function to filter lines that include words in kw_list"""


def filters(line):  # Function filters DF if text has word in list
    return any(word in line.text for word in kw_list)


"""Scrub function.  Iterates through the directory one day at a time, creates a filtered dataset containing tweets where"""
"""the companies of interest are mentioned.  A map reduce transformation is applied to this filtered dataset"""
"""and the reduced key/value pairs are again filtered to only return keys which appear in the keyword list"""
"""This is then turned into a dictionary which contains daily mentions of each company"""
def day_parse(folder_list):
    day_counts = {}
    count = 1
    for folder in folder_list:
        day = folder + '/*/*.json.bz2'  # appends json.bz2 file extension to every folder in dir.
        df = spark.read.json(day)  # Create pyspark dataframe of an entire day's worth of twitter data
        df = df.select(df.created_at, df.text).filter(df.lang == 'en')
        filtered_RDD = df.rdd.filter(filters)  # creates an RDD with contains only the tweets where our companies are mentioned
        filtered_words = filtered_RDD.flatMap(lambda x: x.text.split(' '))\  #tokenizes, maps, and reduces the text in our RDD.  Returns Key/Value pairs for every word
                .map(lambda x: (x,1)) \
                .reduceByKey(lambda x,y: x+y)
        filtered_words = filtered_words.collect()  # Transforms reduced key/value pairs into a python list
        filtered_words = dict(filtered_words)  # transforms key/value python list into python dictionary
        kw_count = [filtered_words.get(key) for key in kw_list]  # Retrieves key/value pairs from dict if the key appears in our keyword list (our companies)
        count_dict =dict(zip(kw_list, kw_count))  # turns these key/value pairs into a dict containing all company mentions from that particular day
        day_counts[count] = count_dict  # assigns those company counts into a new dictionary
        #filtered_RDD.saveAsTextFile(count)
        count += 1

"""Extremely basic trading function.  Compares number of tweets from a particular company between two consecutive days."""
"""With the assumption that twitter volume positively correlates with stock price, this function instructs to buy a stock if its twitter volume decreases"""
"""and sell if its twitter volume increases."""

def buysell(day_counts):
    for item in day_counts.items():
        counts= item[1]
        for word in counts:
                if day_counts[int(day)][word] < day_counts[int(day)+1][word]:
                    print("Sell! More counts tomorrow means higher price.")
                elif day_counts[int(day)][word] > day_counts[int(day)+1][word]:
                    print("Buy! Stock is going down")
                else:
                    print("Stand pat")

"""Intermediate function.  The dictionary of daily company mentions returns a boolean None value if there were no mentions."""
"""This function converts all instances of None into integer 0 so it can interact with other integers."""

def int_maker(day_counts):
    for item in day_counts.items():
        counts = item[1]
        for token in counts:
            if counts.get(token) is None:
                counts[token] = 0
    return day_counts

"""ratio of each company's twitter mentions to all company mentions for that day"""
"""Example: Company A ratio = Company A twitter mentions / sum(all company twitter mentions)"""

def ratio(day_counts):
    day_counts = int_maker(day_counts)
    for item in day_counts.items():
        counts= item[1]
        for company in counts:
            total = sum(counts.values())
            company_proportion[company] = counts.get(company) / total
    return company_proportion


"""For sentiment analysis:  Reads a list of word tokens and searches for them in the posneg dictionary.  Returns all words that appear in dict. """
def in_posneg(word_list):
    list=[]
    for word in word_list:
        if str(word).upper() in posneg:
            list.append(word)
    return list

"""Creates a score of each tweet based on how many positive or negative words appear in the tweet."""
"""Each negative word is -1, positive is +1, and 0 for words that are neutral or do not appear in dict"""
def get_sentiment(list):
    in_dict=in_posneg(list)
    score=0
    for word in in_dict:
        word=word.upper()
        if posneg[word]['pos'] != 0:
            score += 1
        elif posneg[word]['neg'] != 0:
            score += -1
        else:
            score +=0
    return score

def weights(day_counts):
    day_weights = []
    day_counts = ratio(day_counts)
    for



"""gets historical closing price data from yahoo finance for specific companies"""

def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices

if __name__=="__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    day_parse(folder_list)
    initial_investment = np.float(5000000000)
    stocks = ['MSFT', 'AMZN', 'SBUX', 'NVDA', 'YHOO']
    data = web.DataReader(stocks, data_source='yahoo', start='04/01/2017')['High']  # must change start argument based on the date you are interested in
    returns = data.pct_change()
    mean_daily_returns = returns.mean()
    cov_matrix = returns.cov()
    weights = ratio.values()
    avail_investment = dict(zip(stocks, initial_investment * weights))  # dictionary of stocks
    whole_shares = [int(a / b) for a, b in zip(avail_investment.values(), open_price.values())]
    close_price = web.DataReader(stocks, data_source='yahoo', start='04/01/2017')['Close']
    one_day = close_price.loc['2017-04-03'].tolist()  ##test
    port_val = sum([a * b for a, b in zip(whole_shares, one_day)])
    annualized_return = round(np.sum(mean_daily_returns * weights) * 252, 2)
    portfolio_std_dev = round(np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights))) * np.sqrt(252), 2)
    sharpe = (annualized_return - .05) / (portfolio_std_dev)

    """Trading Strategy; use stored count variables to adjust weights"""
    ##get array of current stock prices at open of day
    open_price = web.DataReader(stocks, data_source='yahoo', start='04/01/2017')['Open']
    open_price = open_price.loc['2017-04-03'].to_dict()  # dict of opening stock prices, use current date
    purchase = [a * b for a, b in zip(weights, open_price.values())]  # multiplies array weights by opening prices
    # reduce initial investment by sum of purchase
    investment = initial_investment - sum(purchase)
    # Daily return, percent change = (close(t)- close(t-1))/100
    rf_rate = .05  # risk-free rate