import os
import sys
import boto3
from botocore.exceptions import ClientError
from configs import KEY, SECRET, DL_REGION, DL_NODE_TYPE, DL_NUM_SLAVES, S3_BUCKET_LOGS


def create_emr_cluster():
    """
    Creates EMR cluster, if none exists already
    """

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)

    # Get existing clusters
    clusters = emr.list_clusters()

    if clusters and clusters['Clusters']:
        for cluster in clusters['Clusters']:
            if (cluster['Status']['State'] != 'TERMINATED_WITH_ERRORS'):
                print("Cluster exists:", cluster)
                return
        print("All found Clusters are terminated. We create a new one.")
    else:
        print("No clusters exists. Creating one.")

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
                #'Ec2KeyName': '', # TODO
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                #'Ec2SubnetId': 'subnet-id', # TODO
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )
    except ClientError as ex:
        print("ClientError:", ex)
    except ConnectionRefusedError as ex:
        print("ConnectionRefusedError:", ex)
    except Exception as ex:
        print("Exception:", ex) 
    else:
        # Everything went well, lets see if we can query the cluster
        clusters = emr.list_clusters()
        if clusters and clusters['Clusters']:
            print("Creating Cluster:", clusters['Clusters'])

    return cluster 


if __name__ == "__main__":
    # Create EMR cluster
    cluster = create_emr_cluster()

    if cluster is not None:
        print("Creating cluster...")
