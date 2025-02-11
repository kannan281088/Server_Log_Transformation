import re

logFile = open("Server_Log_Transformation/Data/mbox.txt", "r")
logData = logFile.read()
logFile.close()

extractedMatch = re.findall(r'From\s([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)\s(\w{3}\s\w{3}\s+\d+\s\d+:\d+:\d+\s\d+)', logData)

print(len(extractedMatch))

#for extractedLine in extractedMatch:
#    print(extractedLine)