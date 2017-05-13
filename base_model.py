import json
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col
import yahoo_finance as yf
import matplotlib as plt
sc = SparkContext()
sq = SQLContext(sc)

kw_list = ['Microsoft', 'MSFT', 'IBM', 'SBUX', 'Starbucks', 'NVDA', 'Nvidia', 'YHOO', 'Yahoo']
with open('/Users/jonathandriscoll/PycharmProjects/ProjectC/posneg.json','r') as infile:  # opens the original Loughran file
    posneg=json.load(infile)



json_list = glob.glob('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/*')
df=spark.read.json('/Users/jonathandriscoll/Desktop/Twitter Data/sample/01/*/*/*.json.bz2')
df=df.filter(df['lang']=='en')
df=df.select(['created_at','text'])
df=tokenizer.transform(df)
SBUX26= df.where(col('created_at').like("%Jan 26 %")).where(col('text').like('%starbucks%'))
filtered_RDD=df.rdd





"""this works"""
def ratio(dict):
    ratio_dict={}
    for key in dict:
        if dict.get(key) is None:
            dict[key]=0
    total_mentions= sum(dict.get(key) for key in dict)
    for key in dict:
        ratio_dict[key] = dict.get(key)/total_mentions
    return ratio_dict

def in_posneg(word_list):
    list=[]
    for word in word_list:
        if str(word).upper() in posneg:
            list.append(word)
    return list

"""applies scorekeeper to each tweet and returns a list of sentiment scores.  WORKS"""
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


def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices


"""splits tweets by day"""

created_at_split = filtered_RDD.flatMap(lambda x: x.created_at.split(' '))
days_split



"""creates a dict of keywords and their counts."""



def day_parse(filedir):
    for folder in filedir:
            day=1
            sc.textFile(folder)
            df = spark.read.json(folder)
            df = df.filter(df['lang'] == 'en')
            df = df.select(['created_at', 'text'])
            filtered_RDD = df.rdd
            filtered_words = filtered_RDD.flatMap(lambda x: x.text.split(' '))\
                    .map(lambda x: (x,1)) \
                    .reduceByKey(lambda x,y: x+y)
            filtered_words = filtered_words.collect()   ###Filtered
            filtered_words = dict(filtered_words)
            kw_count = [filtered_words.get(key) for key in kw_list]
            count_dict =dict(zip(kw_list, kw_count))








sentiments= filtered_RDD.flatMap(lambda x: x.text.split(' '))\





