# Import statements
import datetime
from blend360_all_star_clickstream_api.datafetch import DataFetch

data_fetch = DataFetch(secret_scope='my-scope',key_name='api-key')

def fetchData(start_date,end_date,destination_bucket, destination_directory, table_name):
    response = data_fetch.fetchData(start_date = start_date, end_date = end_date, destination_bucket = destination_bucket, destination_directory = destination_directory, table_name = table_name)
    if response.status_code == 200:
    # Get job status from response JSON
        data = response.json()
        jobID = data['job_id']
  
        print('Your JobID is ' + str(jobID) )
        print('You can use this to check your Job Status by going to the second option from the menu.')
        statusCheck(jobID)
        return jobID
    else:
        print("Error:", response.text)

def statusCheck(jobID):
    response = data_fetch.checkStatus(job_id = str(jobID))
    
    if response.status_code == 200:
        data = response.json()
        # Get job status from response JSON
        return data['execution_status']
    else:
        print("Error:", response.text)

def updateAPIKey():
    data_fetch.updateAPIKey()

def deleteAPIKey():
    data_fetch.deleteAPIKey()

print('Welcome to ClickStream Data Analysis\n')
print('1. Fetch Data')
print('2. View Job Status')
print('3. Update API Key')
print('4. Delete API Key')

x = 0
while x == 0 :
    choice = int(input('\nEnter Choice :'))
    if choice == 1:

        # Prompt user to input start date
        start_date_str = input("Please enter the start date (YYYY-MM-DD): ")
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        print(start_date.month)
        # Prompt user to input end date
        end_date_str = input("Please enter the end date (YYYY-MM-DD): ")
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
        print(end_date)
        # Prompt user to input destination bucket
        dest_bucket = input("Please enter the destination bucket: ")

        # Prompt user to input destination directory
        dest_directory = input("Please enter the destination directory: ")

        # Prompt user to input table name
        table_name = input("Please enter the Table Name: ")

        # Print the inputs
        #print("Start date:", start_date)
        #print("End date:", end_date)
        #print("Destination bucket:", dest_bucket)
        #print("Destination directory:", dest_directory)

        try:
            #Call fetchData
            jobID = fetchData(start_date= start_date, end_date=end_date, destination_bucket= dest_bucket, destination_directory= dest_directory, table_name = table_name)
            statusCheck(jobID)
        except Exception as e:
           print("Error:", str(e))

    elif choice == 2:

        # Prompt user to enter job ID
        jobID = input("Please enter the job ID: ")
        try:
            # Call statusCheck
            status = statusCheck(jobID=jobID)
            print('The Job is ' + status)
   
        except Exception as e:
            print("Error:", str(e))

            # Add more code here as needed
            
    elif choice == 3:
        try:
            # Call updateAPIKey
            updateAPIKey()

        except Exception as e:
            print("Error:", str(e))

        print("\nAPI Key Updated Succesfully")
        
    elif choice == 4:
        try:
            # Call deleteAPIKey
            deleteAPIKey()

        except Exception as e:
            print("Error:", str(e))

        print("\nAPI Key Deleted Succesfully")

    elif choice == 5:
        print("\nProgram Exited")
        exit()
        
    else:
        # Code block to execute if choice is not 1, 2, 3, or 4
        print("Invalid choice. Please enter a number between 1 and 5.")
        # Add more code here as needed

