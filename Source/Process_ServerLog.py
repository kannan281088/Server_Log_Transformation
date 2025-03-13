import re
import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import credentials 

lstMail = list()
#mySQLconnection = mysql.connector

#MongoDB setup
def mongo_SetUp():  
  myMongouri = credentials.Mongouri
  #Create a new client and connect to the server
  myMongoClient = MongoClient(myMongouri, server_api=ServerApi('1'))
  myMongodb = myMongoClient.ProjectDB
  records = myMongodb.test
  return records

#MySQL Setup
def mySql_SetUp():  
  mySQLconnection = mysql.connector.connect(**credentials.mySQLConString)
  #cursor = mySQLconnection.cursor()
  return mySQLconnection

# Function to create the SQLite table based on the MongoDB data structure
def create_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_history3 (
            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
            MailID TEXT,
            MailDate DATETIME
        );
    ''')
    #mySQLconnection.commit()

def fetch_Records(mySQLcursor, Qry):
    mySQLcursor.execute(Qry)
    out = mySQLcursor.fetchall()
    from tabulate import tabulate
    print(tabulate(out,headers=[i[0] for i in mySQLcursor.description],
showindex="always",tablefmt='psql'))

myMongoRecords = mongo_SetUp()
mySQLconnection = mySql_SetUp() 
mySQLcursor = mySQLconnection.cursor()

create_table(mySQLcursor)


#Reading the log file
logFile = open("Projects/Server_Log_Transformation/Data/mbox.txt", "r")
logData = logFile.read()
logFile.close()

#Regex to capture email address and date
extractedMatch = re.findall(r'From\s([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)\s(\w{3}\s\w{3}\s+\d+\s\d+:\d+:\d+\s\d+)', logData)

#Moving the matched data to list 
for extractedLine in extractedMatch:           
    lstMail.append({'MailID':extractedLine[0], 'Date': str(datetime.datetime.strptime(extractedLine[1], '%a %b %d %H:%M:%S %Y'))})    

#Removing duplicate data
unique_maillist = []
[unique_maillist.append(x) for x in lstMail if x not in unique_maillist]

#Inserting records to MongoDB
myMongoRecords.insert_many(unique_maillist)#uncomment

#Selecting the records from MongoDB
selMailData = myMongoRecords.find({},{"_id": 0})
#print(list(x))


for mailData in selMailData:
  eMailID =  mailData.get("MailID", None)
  eMailDate = mailData.get("Date", None)

  insert_query = "INSERT INTO user_history3 (MailID, MailDate) VALUES (%s, %s)"
  values = (eMailID, eMailDate)
  mySQLcursor.execute(insert_query, values) #uncomment
  mySQLconnection.commit()
  #print(f"Inserted document: {mailData}")
  #print(mailData)

#List all unique email addresses
fetch_Records(mySQLcursor, "SELECT DISTINCT MailID FROM `user_history3`")

#Count the number of emails received per day
fetch_Records(mySQLcursor, "SELECT Cast(MailDate as Date) 'Date', COUNT(*) as 'Number of emails' FROM `user_history3` GROUP by Cast(MailDate as Date)")

#Find the first and last email date for each email address
fetch_Records(mySQLcursor, "SELECT MailID, Min(MailDate) AS 'First Mail Date', MAX(MailDate) as 'Last Mail Date' FROM `user_history3` GROUP by MailID;")

#Count the total number of emails from each domain (e.g., gmail.com, yahoo.com)
fetch_Records(mySQLcursor, "SELECT SUBSTR(MailID, INSTR( MailID, '@' ) + 1,LENGTH(MailID)) as 'Domain', COUNT(*) as 'Number of Emails' FROM `user_history3` GROUP by SUBSTR(MailID, INSTR( MailID, '@' ) + 1,LENGTH(MailID))")

mySQLconnection.close()