# learning
learning and preparation


# Get CloudWatch metrics data points with python

Function Name [Mandatory]
Start Time [Optional] à default start time(ISO format) is 24 hours past to current time.
End Time [Optional] à default end time(ISO format) is current time
Report path [Optional]à default path is <current directory>/data_points.csv
Validation added for Command line arguments


- The project multiple reusable modules like storage, data-processing and replication

# How to use

* Clone the repository

 

    ```

    git clone 

    ```
* Set AWS environment variables to your accounts

	ACCESS_KEY
	SECRET_KEY

* Run below command with python3.7

    ```

    Python3.7 get_metrics_data.py -f $LAMBDA_FUNC -s $START_TIME -e $END_TIME -r $REPORT_PATH

    ```

Note: Python version 3.7 to be used to run the script.
Reason: datetime.fromisoformat() function I have used to validate ISO date, This function is added in Python 3.7