import os
import sys
import boto3
from botocore.exceptions import ClientError
from configs import KEY, SECRET, DL_REGION, DL_NODE_TYPE, DL_NUM_SLAVES, S3_BUCKET_LOGS, S3_BUCKET_NAME


def terminate_emr_clusters():
    """
    Terminates existing EMR clusters
    """

    # Deleting job files from an S3 bucket
    s3 = boto3.resource('s3',
                        region_name=DL_REGION, 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,)

    s3.Object(S3_BUCKET_NAME, "jobs/etl.py").delete()
    s3.Object(S3_BUCKET_NAME, "jobs/dl.cfg").delete()
    s3.Object(S3_BUCKET_NAME, "jobs/keys.cfg").delete()
    s3.Object(S3_BUCKET_NAME, "jobs/configs.py").delete()

    print("PyStark ETL job Files deleted.")

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)

    # Get existing clusters
    clusters = emr.list_clusters()

    if not clusters or not clusters['Clusters']:
        print("No clusters exists. Creating one.")
        return
    else:
        for cluster in clusters['Clusters']:
            print("Cluster exists:", "Id:", cluster['Id'], "Name:", cluster['Name'])
            response = emr.terminate_job_flows(JobFlowIds=[cluster['Id'],])
            print("Terminating:", response)


if __name__ == "__main__":
    # Terminate EMR clusters
    terminate_emr_clusters()
