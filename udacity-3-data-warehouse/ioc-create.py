# Load in all required libraries
import pandas as pd 
import boto3
import json
import configparser


# Open and read the contents of the config file
config = configparser.ConfigParser()
config.read_file(open('./dwh-ioc.cfg'))

# Load all the keys needed to create AWS services
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_REGION             = config.get("DWH","DWH_REGION")
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_client(name, func):
    return func(name,
                region_name=DWH_REGION,
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET)


def main():
    # Creating resources/clients for all needed infrastructure: EC2, S3, IAM, Redshift
    ec2 = create_client('ec2', boto3.resource)
    s3 = create_client('s3', boto3.resource)
    iam = create_client('iam', boto3.client)
    redshift = create_client('redshift', boto3.client)
    
    # End of main
    pass


if __name__ == "__main__":
    main()