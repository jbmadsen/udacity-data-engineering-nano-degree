import os
import sys
import boto3
from botocore.exceptions import ClientError
import configparser


def create_default_roles():
    """
    Creates EMR_DefaultRole and EMR_EC2_DefaultRole if they don't exist
    """
    iam = boto3.client('iam',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)
    pass


def create_emr_cluster():
    """
    Creates EMR cluster
    """

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)


    # Creating cluster settings: InstanceGroups
    instance_groups = [
        {
            'Name': "Master",
            'Market': 'SPOT',
            'InstanceRole': 'MASTER',
            'InstanceType': DL_NODE_TYPE,
            'InstanceCount': 1,
        },
        {
            'Name': "Slave",
            'Market': 'SPOT',
            'InstanceRole': 'CORE',
            'InstanceType': DL_NODE_TYPE,
            'InstanceCount': int(DL_NUM_SLAVES),
        }
    ]

    try:
        cluster = None
        # Creating cluster
        cluster = emr.run_job_flow(
            Name='emr_cluster',
            LogUri=S3_BUCKET_LOGS,
            ReleaseLabel='emr-5.20.0',
            Applications=[
                {'Name': 'Spark'},
            ],
            Instances={
                'InstanceGroups': instance_groups,
                'Ec2KeyName': '',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-id',
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
    except ClientError as ex:
        print(ex)

    return cluster 


if __name__ == "__main__":
    # Set path to current directory
    os.chdir(os.path.dirname(sys.argv[0]))

    # Open and read the contents of the config file
    ioc_config = configparser.ConfigParser()
    ioc_config.read_file(open('./dl.cfg'))

    # Load all the keys needed to create AWS services
    KEY             = ioc_config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET          = ioc_config.get('AWS','AWS_SECRET_ACCESS_KEY')

    DL_REGION       = ioc_config.get("DL","DL_REGION")
    DL_NUM_SLAVES   = ioc_config.get("DL","DL_NUM_SLAVES")
    DL_NODE_TYPE    = ioc_config.get("DL","DL_NODE_TYPE")

    S3_BUCKET_LOGS  = ioc_config.get("S3","S3_BUCKET_LOGS")

    # Create EMR cluster
    cluster = create_emr_cluster()

    if cluster is not None:
        print("Creating cluster... I think?")
