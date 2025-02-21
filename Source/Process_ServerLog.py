import re
import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector

lstMail = list()
#mySQLconnection = mysql.connector

#MongoDB setup
def mongo_SetUp():  
  myMongouri = "mongodb+srv://kanna28pgp:Pass1234@cluster0.l31kn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
  #Create a new client and connect to the server
  myMongoClient = MongoClient(myMongouri, server_api=ServerApi('1'))
  myMongodb = myMongoClient.test
  records = myMongodb.Project3
  return records

#MySQL Setup
def mySql_SetUp():  
  mySQLconnection = mysql.connector.connect(
    host = "localhost",
    port = 3306,
    user = "root",
    password = "",
    database = "test"  
  )
  #cursor = mySQLconnection.cursor()
  return mySQLconnection

myMongoRecords = mongo_SetUp()
mySQLconnection = mySql_SetUp() 
mySQLcursor = mySQLconnection.cursor()

# Function to create the SQLite table based on the MongoDB data structure
def create_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_history1 (
            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
            MailID TEXT,
            MailDate DATETIME
        );
    ''')
    #mySQLconnection.commit()


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
#records.insert_many(unique_maillist)

#Selecting the records from MongoDB
selMailData = myMongoRecords.find({},{"_id": 0})
#print(list(x))


for mailData in selMailData:
  eMailID =  mailData.get("MailID", None)
  eMailDate = mailData.get("Date", None)

  insert_query = "INSERT INTO user_history1 (MailID, MailDate) VALUES (%s, %s)"
  values = (eMailID, eMailDate)
  mySQLcursor.execute(insert_query, values)
  mySQLconnection.commit()
  #print(f"Inserted document: {mailData}")

  print(mailData)

mySQLconnection.close()





