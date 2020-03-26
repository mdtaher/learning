import boto3
from datetime import datetime
import csv

class DataPointsCollector(object):
    """
    """
    RESPONSE_KEY = 'MetricDataResults'

    def __init__(self, function_name, start_timestamp, end_timestamp):
        if not function_name:
            raise ValueError("Please provid function name, it should not be empty")
        self.function_name = function_name
        if start_timestamp:
            self.start_timestamp = start_timestamp
        if end_timestamp:
            self.end_timestamp = end_timestamp
        ## CloudWatch client 
        self.client = boto3.client('cloudwatch')


    #def get_metric_info(self, event, context): This defination is for lambda
    def get_metric_info(self):
        """
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
        """

        keys = data_points[0].keys()
        with open('data_points.csv', 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data_points)


if __name__ == "__main__":

    #create instance of DataPointsCollector
    # DataPointsCollector class accepts 3 params to intialize
    # 1. function name  2. Start Date time   3. End Date time 
    # Datetime should be in ISO format.

    data_object = DataPointsCollector('test-get-metric', '2020-03-22T01:58:0000', '2020-03-24T02:58:0000')
    data_points = data_object.get_metric_info()
    data_object.write_to_csv(data_points)
