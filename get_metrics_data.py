import boto3
from datetime import datetime, timedelta
import csv
import argparse
import sys
import os
from enum import Enum


class ErrorCode(Enum):
    """Enumerates the error codes."""

    NO_ERROR = 0
    MANDATORY_PARAM_MISSING = 100
    WRONG_DATE = 101

class MetricsUtil(object):
    """Encapsulates utility functions."""

    @staticmethod
    def bail_out(exit_code=ErrorCode.NO_ERROR):
        """
        Bail out on failure.

        :param exit_code ExitCode: Exit code
        """
        sys.exit(exit_code.value)

    @staticmethod
    def validate_date(start_date_str, end_date_str):
        """
        Validate dates and compare start and end dates.

        :param start_date_str str: datetime string for start time
        :param end_date_str str: datetime string for end time

        :rtype: bool
        """
        # NOTE: datetime.fromisoformat() New in version Python 3.7

        try:
            start_date_iso = datetime.fromisoformat(start_date_str)
            end_date_iso = datetime.fromisoformat(end_date_str)
            return True
        except ValueError as date_error:
            print(f"Incorrect data format, datetime should be in ISO format, {date_error}")
            return False
        if start_date_iso > end_date_iso:
            return False

        return True

    @staticmethod
    def validate(env):
        """
        validates arguments received in Parser object

        :param env dict: Parser argumensts to be validated

        :rtype: bool
        """
        exit_code = ErrorCode.NO_ERROR
        if not env.function_name:
            print('Mandatory parameter (function_name) is missing')
            PARSER.print_help()
            exit_code = ErrorCode.MANDATORY_PARAM_MISSING
        if not MetricsUtil.validate_date(env.start_datetime, env.end_datetime):
            PARSER.print_help()
            exit_code = ErrorCode.WRONG_DATE
        if exit_code != ErrorCode.NO_ERROR:
            MetricsUtil.bail_out(ErrorCode.MANDATORY_PARAM_MISSING)
        
        return True
            
    

class DataPointsCollector(object):
    """
    It will collect data points from cloudwatch for given Lambda function for given time.

    """
    RESPONSE_KEY = 'MetricDataResults'
    DEFAULT_REPORT_FILE_NAME = "data_points.csv"

    def __init__(self, fun_name, start_time, end_time, re_path):
        """
        Initializes DataPointsCollector class 

        :param fun_name str: Name of Lambda function
        :param start_time str: Start time for data points report
        :param end_time str: End time for data points report
        :param re_path str: Full path of report file
        """
        self.function_name = fun_name
        self.start_timestamp = start_time
        self.end_timestamp = end_time
        self.report_path = re_path
        ## CloudWatch client 
        self.client = boto3.client('cloudwatch')


    #def get_metric_info(self, event, context): This defination is for lambda
    def get_metric_info(self):
        """
        This function will collect CloudWatch metric data

        :return MetricDataResults List for report
        :rtype: list
        """
        metric_data_object = self.client.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "cdbdata_invocations",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Lambda",
                            "MetricName": "Invocations",
                            "Dimensions": [
                                {
                                    "Name": "FunctionName",
                                    "Value": self.function_name
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    },
                    "ReturnData": True
                },
                {
                    "Id": "cdbdata_errors",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Lambda",
                            "MetricName": "Errors",
                            "Dimensions": [
                                {
                                    "Name": "FunctionName",
                                    "Value": self.function_name
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    },
                    "ReturnData": True
                },
                {
                    "Id": "cdbdata_throttles",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Lambda",
                            "MetricName": "Throttles",
                            "Dimensions": [
                                {
                                    "Name": "FunctionName",
                                    "Value": self.function_name
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    },
                    "ReturnData": True
                },
                {
                    "Id": "cdbdata_concurrentexec",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Lambda",
                            "MetricName": "ConcurrentExecutions",
                            "Dimensions": [
                                {
                                    "Name": "FunctionName",
                                    "Value": self.function_name
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    },
                    "ReturnData": True
                }
            ],
            StartTime=self.start_timestamp,
            EndTime=self.end_timestamp,
            ScanBy='TimestampDescending'
        )

        metric_data_points = metric_data_object[DataPointsCollector.RESPONSE_KEY]

        return metric_data_points

    def write_to_csv(self, data_points):
        """
        Writes data to CSV file

        :param data_points list: CloudWatch data points list
        """
        keys = data_points[0].keys()
        with open(self.report_path, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data_points)



if __name__ == "__main__":

    # create instance of DataPointsCollector
    # DataPointsCollector class accepts 3 params to intialize
    # 1. function name  2. Start Datetime   3. End Datetime 
    # Datetime should be in ISO format.

    # If Dates are not provided through arguments, by default start date is one day previous date
    past_one_day_date_time = (datetime.now() - timedelta(days = 1)).isoformat()

    # Default system path where file report to be generated
    default_report_path = os.getcwd() + os.path.sep + DataPointsCollector.DEFAULT_REPORT_FILE_NAME

    # Adding command line arguments to parser object

    PARSER = argparse.ArgumentParser(
        description='DSIP CloudWatch Datapoints collector')

    PARSER.add_argument(
        '-f',
        '--function-name',
        dest='function_name',
        type=str,
        help='[M] Lambda function name')
    PARSER.add_argument(
        '-s',
        '--start-datetime',
        dest='start_datetime',
        type=str,
        default=past_one_day_date_time,
        help='[O] Start Datetime, should be before End Datetime,in ISO format')
    PARSER.add_argument(
        '-e',
        '--end-datetime',
        dest='end_datetime',
        type=str,
        default=datetime.now().isoformat(),
        help='[O] END Datetime, should be  after Start Datetime,in ISO format')
    PARSER.add_argument(
        '-r',
        '--report-path',
        dest='report_path',
        type=str,
        default=default_report_path,
        help='[O] Report path where report to be created')


    ENV = PARSER.parse_args()

    if MetricsUtil.validate(ENV):
        print(f'Datapoints report to be fetched for {ENV}')
        data_object = DataPointsCollector(
            ENV.function_name, ENV.start_datetime, ENV.end_datetime, ENV.report_path)
        data_points = data_object.get_metric_info()
        data_object.write_to_csv(data_points)
        MetricsUtil.bail_out(ErrorCode.NO_ERROR)
