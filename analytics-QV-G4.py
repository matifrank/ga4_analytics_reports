import logging
import json
import os
import csv
# Importing all libraries and custom functions from the `utils.py` module
from modules.utils import *

scriptDir = os.path.dirname(os.path.realpath(__file__)) + os.sep
logging.basicConfig(filename='logs/analytics-QV.log',level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

# Load config report conditions
with open('config/analytics-G4-QV.config.json', 'r') as f:
    config = json.load(f)

# Initialize the analytics reporting service client credentials 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
client = BetaAnalyticsDataClient()

#date condition for reporte generation
todayDate = datetime.now().date()
yesterdayDate = todayDate - timedelta(1)
firstDateOfMonth = yesterdayDate.replace(day=1)
#first of current month until yesterday date
start = yesterdayDate.replace(day=1).strftime('%Y-%m-%d')
end = (todayDate - timedelta(1)).strftime('%Y-%m-%d')
#before yesterday date for files deleting
bend = (todayDate - timedelta(2)).strftime('%Y-%m-%d')

def main():
    # process batch request to analytics
    resulting_response = batch_report(config, client, start, end)
    
    #convert response into dataframe 
    df  = df_report(resulting_response)
    
    #process dataframe and export into separates csv files by config and delete previous file
    df_qvcsv(df, config, start, end, bend)
    
if __name__ == '__main__':
	main()