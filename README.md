# QubolePysdkExamples
Qubole pysdk examples
Qubole Data Service Python SDK
Build Status
A Python module that provides the tools you need to authenticate with, and use the Qubole Data Service API.

Installation
From PyPI
The SDK is available on PyPI.

$ pip install qds-sdk
From source
Get the source code:

Either clone the project: git clone git@github.com:qubole/qds-sdk-py.git and checkout latest release tag from Releases.
Or download one of the releases from https://github.com/qubole/qds-sdk-py/releases
Run the following command (may need to do this as root):

$ python setup.py install
Alternatively, if you use virtualenv, you can do this:

$ cd qds-sdk-py
$ virtualenv venv
$ source venv/bin/activate
$ python setup.py install
This should place a command line utility qds.py somewhere in your path

$ which qds.py
/usr/bin/qds.py
CLI
qds.py allows running Hive, Hadoop, Pig, Presto and Shell commands against QDS. Users can run commands synchronously - or submit a command and check its status.

$ qds.py -h  # will print detailed usage
Examples:

run a hive query and print the results

$ qds.py --token 'xxyyzz' hivecmd run --query "show tables"
$ qds.py --token 'xxyyzz' hivecmd run --script_location /tmp/myquery
$ qds.py --token 'xxyyzz' hivecmd run --script_location s3://my-qubole-location/myquery
pass in api token from bash environment variable

$ export QDS_API_TOKEN=xxyyzz
run the example hadoop command

$ qds.py hadoopcmd run streaming -files 's3n://paid-qubole/HadoopAPIExamples/WordCountPython/mapper.py,s3n://paid-qubole/HadoopAPIExamples/WordCountPython/reducer.py' -mapper mapper.py -reducer reducer.py -numReduceTasks 1 -input 's3n://paid-qubole/default-datasets/gutenberg' -output 's3n://example.bucket.com/wcout'
check the status of command # 12345678

$ qds.py hivecmd check 12345678
{"status": "done", ... }
If you are hitting api_url other than api.qubole.com, then you can pass it in command line as --url or set in as env variable

$ qds.py --token 'xxyyzz' --url https://<env>.qubole.com/api hivecmd ...

or

$ export QDS_API_URL=https://<env>.qubole.com/api
SDK API
An example Python application needs to do the following:

Set the api_token and api_url (if api_url other than api.qubole.com):

from qds_sdk.qubole import Qubole

Qubole.configure(api_token='ksbdvcwdkjn123423')

# or

Qubole.configure(api_token='ksbdvcwdkjn123423', api_url='https://<env>.qubole.com/api')
Use the Command classes defined in commands.py to execute commands. To run Hive Command:

from qds_sdk.commands import *

hc=HiveCommand.create(query='show tables')
print "Id: %s, Status: %s" % (str(hc.id), hc.status)
example/mr_1.py contains a Hadoop Streaming example

Reporting Bugs and Contributing Code
Want to report a bug or request a feature? Please open an issue.
Want to contribute? Fork the project and create a pull request with your changes against unreleased branch.
