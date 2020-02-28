# Load in all required libraries
import os
import sys
import pandas as pd 
import boto3
import botocore.exceptions
import json
import configparser
import time


# Set path to current directory
os.chdir(os.path.dirname(sys.argv[0]))


# Open and read the contents of the config file
ioc_config = configparser.ConfigParser()
ioc_config.read_file(open('./dwh-iac.cfg'))

# Load all the keys needed to create AWS services
KEY                    = ioc_config.get('AWS','KEY')
SECRET                 = ioc_config.get('AWS','SECRET')

DWH_REGION             = ioc_config.get("DWH","DWH_REGION")
DWH_CLUSTER_TYPE       = ioc_config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = ioc_config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = ioc_config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = ioc_config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = ioc_config.get("DWH","DWH_DB")
DWH_DB_USER            = ioc_config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = ioc_config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = ioc_config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = ioc_config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_client(name, func):
    """Creating resources/clients for all needed infrastructure: EC2, S3, IAM, Redshift

    Keyword arguments:
    name -- the name of the AWS service resource/client 
    func -- the boto3 function object (e.g. boto3.resource/boto3.client) 
    """
    print("Creating client for", name)
    return func(name,
                region_name=DWH_REGION,
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET)


def prettyRedshiftProps(props, limited = True):
        #pd.set_option('display.max_colwidth', -1)
        if limited:
            keysToShow = ["ClusterStatus"]
        else:
            keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])


def main():
    """Destroying the Redshift cluster created with ioc-create.py"""
    
    iam = create_client('iam', boto3.client)
    redshift = create_client('redshift', boto3.client)
    
    # Delete cluster (will take time)
    resp = redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,   
                                   SkipFinalClusterSnapshot=True)
    print("Deleting Redshift cluster")

    # Query the status - I have no idea what the status will become after deletion, so no loop here
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    df = prettyRedshiftProps(myClusterProps, limited=False)
    print(df)

    # Detach and delete role, since there are no cluster to use this on
    detach_resp = iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    delete_resp = iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print("Detaching and deleting roles from cluster")
    
    # TODO: Print status of these, wait for cluster to be deleted, and confirm
    
    # End of main
    return


if __name__ == "__main__":
    main()