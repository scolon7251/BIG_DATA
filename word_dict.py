import sys
import csv

"""argv[1] is the filepath of the Loughran sentiment dictionary"""

"""argv[2] is the destination filepath for the written csv file"""

"""note that I first edited the original Loughran  dataset to include only 'Words','Negative', and 'Positive' entries"""

f=open(sys.argv[1])  # opens the original Loughran file
reader=csv.DictReader(f)  # creates an iterable object where each line is a dictionary
d=open(sys.argv[2],'w')  # opens a new .csv file where argv[2] is the filepath where you want to write to
fieldnames=['Word','Negative','Positive']  # dict keys for new file
writer=csv.DictWriter(d, fieldnames=fieldnames)
for row in reader:
    if row.get('Positive') != '0' or row.get('Negative') != '0': #only writes entries that have a positive or negative sentiment
        writer.writerow(row)
f.close()
d.close()




