# ADD CODE HERE
# Import statements
import datetime
from blend360_all_star_clickstream_api.datafetch import DataFetch

data_fetch = DataFetch(secret_scope='my-scope',key_name='api-key')

def fetchData(start_date,end_date):
    response = data_fetch.fetchData(start_date = datetime.date(2020, 1, 1), end_date = datetime.date(2022, 31, 12), destination_bucket= "clickstream-server", destination_directory = "poop_test")
    response.json()

def statusCheck(jobID):
    response = data_fetch.checkStatus(12345)
    response.json()

def updateAPIKey():
    data_fetch.updateAPIKey()

def deleteAPIKey():
    data_fetch.deleteAPIKey()