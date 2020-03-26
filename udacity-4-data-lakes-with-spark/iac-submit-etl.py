import os
import sys
import boto3
from botocore.exceptions import ClientError
from configs import KEY, SECRET, DL_REGION, S3_BUCKET_JOBS, S3_BUCKET_LOGS, S3_BUCKET_NAME


# Steps:
# 1) Check we have a running cluster
# 2) Pack up the scripts needed and send to S3
# 3) Create a new job-step using boto3


def main():
    """
    Submit etl.py and required files to be run on active EMR cluster, if one such exists
    """
    # 1) Check we have a running cluster

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)
    
    # Get existing clusters
    clusters = emr.list_clusters()
    job_flow_id = None
    job_flow_state = None

    if clusters and clusters['Clusters']:
        for cluster in clusters['Clusters']:
            # Possible states: https://www.knowledgepowerhouse.com/what-are-different-states-in-aws-emr-cluster/373
            job_flow_state = cluster['Status']['State']
            if job_flow_state in ['RUNNING', 'WAITING']:
                job_flow_id = cluster['Id']
                break

    if job_flow_id is None:
        print("No active clusters found. Exiting.")
        return

    # We have the Id of the cluster to submit the job to        
    print("Active EMR Cluster found. Uploading files and adding Job set to Cluster")

    # 2) Pack up the scripts needed and send to S3

    # upload file to an S3 bucket
    s3 = boto3.resource('s3',
                        region_name=DL_REGION, 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,)

    s3.meta.client.upload_file("etl.py", S3_BUCKET_NAME, "jobs/etl.py")
    s3.meta.client.upload_file("dl.cfg", S3_BUCKET_NAME, "jobs/dl.cfg")
    s3.meta.client.upload_file("keys.cfg", S3_BUCKET_NAME, "jobs/keys.cfg")
    s3.meta.client.upload_file("configs.py", S3_BUCKET_NAME, "jobs/configs.py")

    print("Scripts uploaded")

    # 3) Create a new job-step using boto3

    # Relevant documentation/support for creating EMR job steps:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html
    # https://stackoverflow.com/questions/36706512/how-do-you-automate-pyspark-jobs-on-emr-using-boto3-or-otherwise
    # https://github.com/terraform-providers/terraform-provider-aws/issues/1866
    # https://stackoverflow.com/questions/31525012/how-to-bootstrap-installation-of-python-modules-on-amazon-emr

    JobSteps = [
        {
            'Name': 'Setup Debugging', # AWS CLI option --enable-debugging. We want those log files!
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['state-pusher-script']}
        },
        {
            'Name': 'Copy files', # Copy files
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['aws', 's3', 'sync', S3_BUCKET_JOBS, '/home/hadoop/']}
        },
        {
            'Name': 'Install dependencies', # Install dependencies
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['sudo','pip','install','configparser']}
        },
        {
            'Name': 'Run ETL job', # Run etl.py
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['spark-submit', '/home/hadoop/etl.py']}
        }
    ]

    print("Submitting to Job flow ID:", job_flow_id)

    step_response = emr.add_job_flow_steps(JobFlowId=job_flow_id, 
                                           Steps=JobSteps)

    step_ids = step_response['StepIds']

    print("Step IDs created:", step_ids)

    pass


if __name__ == "__main__":
    main()

