import bz2
from bz2 import decompress
import fnmatch
import os
import pickle
import glob


##UPDATED 4/21-- USING GLOB TO READ ALL JSON FILES FROM 01/26 INTO ONE LARGE RDD

spark = SparkContext()
os.chdir('/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/00')

#get list of all json.bz2 files (1410 files total)
json_list = glob.glob('/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/*/*.json.bz2')
##Read ALL Files into RDD
huge_RDD = sc.textFile(','.join(json_list))

##Or, split into a few lists and make multiple RDDS
chunks = [json_list[x:x+100] for x in xrange(0, len(json_list), 100)]


##Walks through directory unzips to json
for dirpath, dirname, files in os.walk('/Users/samuelcolon/Documents/Baruch/Big_Data/Assignment_C/sample/01/26/00'):
    for filename in files:
        if filename.endswith('.json.bz2'):
            filepath = os.path.join(dirpath, filename)
            newfilepath = os.path.join(dirpath, filename[:-4])
            print(filepath)
            print(newfilepath)
            with open(newfilepath, 'wb') as new_file, bz2.BZ2File(filepath, 'rb') as file:
                for data in iter(lambda : file.read(100 * 1024), b''):
                    new_file.write(data)




##Loop through tweets, filter to read in only those that have 'lang' = 'en'

practice_file = sqlContext.read.json('30.json')
practice_file.printSchema()

##Jon's function for reading Json files
def json_reader(str):
    with open(str) as json_file:
        json_data = json.load(json_file)