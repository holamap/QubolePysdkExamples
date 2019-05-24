"""
This is the sample code used for submitting a SparkCommand and getting the result back to local file.
Similar way can be followed for HiveCommand etc.
"""

import sys
import string
from ConfigParser import SafeConfigParser
from qds_sdk.qubole import Qubole
from qds_sdk.commands import *
import boto
import time


# Used for generating file name to download the result
def get_random_filename(size=10):
    return "/tmp/result_" + str(int(time.time())) + ".tsv"

# Returning content from the file 
def get_content(filename):
    with open(filename, 'r') as content_file:
        content = content_file.read()
    return content

# Executing given query
def execute_cmd(command):
    if command is None or command == "":
        return None
    cmd = SparkCommand.create(cmdline=command)
    query_id = str(cmd.id)
    print "Starting Command with id: " + query_id + "\nProgress: =>",
    while not SparkCommand.is_done(cmd.status):
        print "\b=>",
        cmd = SparkCommand.find(cmd.id)
        time.sleep(5)
    print cmd.get_log()
    if SparkCommand.is_success(cmd.status):
        print "\nCommand Executed: Completed successfully"
    else:
        print "\nCommand Executed: Failed!!!. The status returned is: " + str(cmd.status)
    return cmd

# Downloading the result
def get_results(command):
    if command is None:
        return None
    filename = get_random_filename(10)
    print filename
    fp = open(filename, 'w')
    command.get_results(fp, delim="\n")
    print "Starting Result fetch with Command id: " + str(command.id) + "\nProgress: =>",
    while not SparkCommand.is_done(command.status):
        print "\b=>",
        time.sleep(5)
    if SparkCommand.is_success(command.status):
        print "\nCommand Executed: Results fetch completed successfully"
    else:
        print "\nCommand Executed: Result fetch for original command " + str(command.id) + "Failed!!!. The status returned is: " + str(command.status)
    fp.close()
    content = get_content(filename)
    return content


if __name__ == '__main__':
    # Setting API token
    Qubole.configure(api_token='<qubole-api-token>',api_url='https://<env>.qubole.com/api')
    get_results(execute_cmd('/usr/lib/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn-client /spark/app-jar/location/app.jar'))
