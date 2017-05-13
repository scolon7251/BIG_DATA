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

##For standalone program
APP_NAME = "November Rain App"

if __name__=="__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

##make root directory a command_line parameter
root_dir = sys.argv[1]

##get working directory
#working_dir = os.getcwd()
test_dir= '/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/*'
folder_list = glob.glob(test_dir)

##Function to filter lines that include words in kw_list
def filters(line):  ##Function filters DF if text has word in list
    return(any(word in line.text for word in kw_list))#Now RDD only has lines which include list above
kw_list = ['Microsoft', 'MSFT', 'IBM', 'SBUX', 'Starbucks', 'NVDA', 'Nvidia', 'YHOO', 'Yahoo']

##try to loop and apply function day by day
##group by date??
##if all data is read at once, do we have to split by date

count_array={}
count = 1
for folder in folder_list:
    day = folder + '/*/*.json.bz2'
    day_DF = spark.read.json(day)
    day_DF = day_DF.select(day_DF.created_at, day_DF.text).filter(day_DF.lang == 'en')
    day_RDD = day_DF.rdd
    filtered = day_RDD.filter(lambda x: filters(x))
    filtered_words = filtered.flatMap(lambda x: x.text.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y)
    filtered_words = filtered_words.collect()  ###Filtered
    filtered_words = dict(filtered_words)
    kw_count = [filtered_words.get(key) for key in kw_list]
    count_dict= dict(zip(kw_list, kw_count))
    count_array[str(count)] = count_dict
    count+=1

<<<<<<< HEAD
d='/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/2017-STAT-9794-November-Rain/posneg.json'
zips = '/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/00/'
json_list = glob.glob('/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/*/*.json.bz2')
=======
def buysell(dict):
    kw_list = ['microsoft', 'Microsoft' 'MSFT', 'IBM', 'Starbucks', 'sbux', 'nvidia', 'nvda', 'yahoo', 'yhoo']
    for item in day_counts.items():
        counts= item[1]
        for word in counts:
            if word in kw_list:
                for day in day_counts:
                    if day_counts[int(day)][word] < day_counts[int(day)+1][word]:
                        print("Sell! More counts tomorrow means higher price.")
                    elif day_counts[int(day)][word] > day_counts[int(day)+1][word]:
                        print("Buy! Stock is going down")
                    else:
                        print("Stand pat")
def ratio(day_counts):
    company_proportion = {}
    daily_ratio = {}
    day_counts = int_maker(day_counts)
    for item in day_counts.items():
        counts= item[1]
        for company in counts:
            total = sum(counts.values())
            company_proportion[company] = counts.get(company) / total
        return company_proportion


for dirpath, dirname, files in os.walk(test_dir):
    print(dirpath)

        '/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/00'):
    for filename in files:
        if filename.endswith('.json.bz2'):
            filepath = os.path.join(dirpath, filename)
            newfilepath = os.path.join(dirpath, filename[:-4])
            print(filepath)
            print(newfilepath)
            with open(newfilepath, 'wb') as new_file, bz2.BZ2File(filepath, 'rb') as file:
                for data in iter(lambda: file.read(100 * 1024), b''):
                    new_file.write(data)




>>>>>>> e8b6ab70dff6fb0bbb8d9a8fadaa0d4229305474



##Need two full days of data first before trading
def get_date():  # get from created_at


"""Start of Financial Model"""
#choose portfolio weights based on proportion company was mentioned:
total = sum(filter(None, kw_count)) ##sums total mentions
company_proportion = {}  #initializes empty dictionary of proportions of mentions
for k, v in count_dict.items():    #populates dictionary with company: proportion
    if v != None:
        company_proportion[k] = float(v)/(total)

##Put dict values into array of weights
weights = []

##Use company proportion for portfolio weights, below:
#Integrate Yahoo API, to compare counts with closing price of selected stocsk
def historical(symbol,startdate,enddate):
    share=yf.Share(symbol)
    history=share.get_historical(startdate,enddate)
    hist_prices=[]
    for day in history:
        hist_prices.append(day.get('Close'))
    return hist_prices

###Financial Model
##Input investment, stock info, data to compute Sharpe Ratio
initial_investment = np.float(5000000000)
reserve_capital =
stocks = ['MSFT', 'AMZN', 'SBUX', 'NVDA', 'YHOO']
data = web.DataReader(stocks,data_source='yahoo',start='04/01/2017')['High']
#Get daily returns
returns = data.pct_change()
mean_daily_returns = returns.mean()
cov_matrix = returns.cov()  #covariance matrix
#set portfolio weights
weights = np.asarray([0.3, 0.2, 0.2, 0.1, 0.2])
#capital allocation
#avail_investment = initial_investment*weights
avail_investment = dict(zip(stocks, initial_investment*weights)) #dictionary of stocks
whole_shares = [int(a/b) for a,b in zip(avail_investment.values(), open_price.values())]

##get current value of portfolio
##shares held * current price for each stock
close_price  = web.DataReader(stocks,data_source='yahoo',start='04/01/2017')['Close']
one_day = close_price.loc['2017-04-03'].tolist()  ##test
port_val = sum([a*b for a,b in zip(whole_shares, one_day)]) #

#annualized return
annualized_return = round(np.sum(mean_daily_returns * weights) * 252,2)
portfolio_std_dev = round(np.sqrt(np.dot(weights.T,np.dot(cov_matrix, weights))) * np.sqrt(252),2)

##COMPUTE SHARPE RATIO
sharpe = (annualized_return - .05)/(portfolio_std_dev)

##Trading Strategy; use stored count variables to adjust weights
##get array of current stock prices at open of day
open_price= web.DataReader(stocks,data_source='yahoo',start='04/01/2017')['Open']
open_price = open_price.loc['2017-04-03'].to_dict() #dict of opening stock prices
#use current_date


purchase = [a*b for a,b in zip(weights, open_price.values())] #multiplies array weights by opening prices
#reduce initial investment by sum of purchase
investment = initial_investment - sum(purchase)

##get day's counts again, adjust weights and update value of portfolio according to closing price?

#Daily return, percent change = (close(t)- close(t-1))/100
rf_rate = .05 #risk-free rate

