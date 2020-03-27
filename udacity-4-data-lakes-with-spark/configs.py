import os
import sys
import configparser


# Set path to current directory
if os.path.dirname(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

# Open and read the contents of the keys file
iac_keys = configparser.ConfigParser()
iac_keys.read_file(open('./keys.cfg'))

# Load all the keys needed to connect to AWS services
KEY             = iac_keys.get('AWS','AWS_ACCESS_KEY_ID')
SECRET          = iac_keys.get('AWS','AWS_SECRET_ACCESS_KEY')

# Open and read the contents of the config file
iac_config = configparser.ConfigParser()
iac_config.read_file(open('./dl.cfg'))

# Load all the keys needed to create the EMR cluster
DL_REGION       = iac_config.get("DL","DL_REGION")
DL_NUM_SLAVES   = iac_config.get("DL","DL_NUM_SLAVES")
DL_NODE_TYPE    = iac_config.get("DL","DL_NODE_TYPE")


S3_BUCKET_NAME  = iac_config.get("S3","S3_BUCKET_NAME")
S3_BUCKET_JOBS  = iac_config.get("S3","S3_BUCKET_JOBS")
S3_BUCKET_LOGS  = iac_config.get("S3","S3_BUCKET_LOGS")
S3_BUCKET_IN   = iac_config.get("S3","S3_BUCKET_IN")
S3_BUCKET_OUT   = iac_config.get("S3","S3_BUCKET_OUT")

# Delete unused objects
del iac_keys
del iac_config
