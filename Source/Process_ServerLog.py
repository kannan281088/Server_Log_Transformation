import re
import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

lstMail = list()
uri = "mongodb+srv://kanna28pgp:Pass1234@cluster0.l31kn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.test
records = db.Project3


#Reading the log file
logFile = open("Server_Log_Transformation/Data/mbox.txt", "r")
logData = logFile.read()
logFile.close()

#Regex to capture email address and date
extractedMatch = re.findall(r'From\s([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)\s(\w{3}\s\w{3}\s+\d+\s\d+:\d+:\d+\s\d+)', logData)

for extractedLine in extractedMatch:           
    lstMail.append({'MailID':extractedLine[0], 'Date': str(datetime.datetime.strptime(extractedLine[1], '%a %b %d %H:%M:%S %Y'))})    

unique_maillist = []
[unique_maillist.append(x) for x in lstMail if x not in unique_maillist]

records.insert_many(unique_maillist)
    
    
    

    
