import os
import sys
import boto3
from botocore.exceptions import ClientError
from configs import KEY, SECRET, DL_REGION, DL_NODE_TYPE, DL_NUM_SLAVES, S3_BUCKET_LOGS


if __name__ == "__main__":
    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=DL_REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)
    
    # Get existing clusters
    clusters = emr.list_clusters()

    if clusters and clusters['Clusters']:
        print("Clusters exists:", )
        print("")
        for cluster in clusters['Clusters']:
            print("Id:", cluster['Id'])
            print("Name:", cluster['Name'])
            print("Status:")
            print("  State:", cluster['Status']['State'])
            print("  StateChangeReason:", cluster['Status']['StateChangeReason'])
            print("  Timeline:")
            print("    Created:", cluster['Status']['Timeline']['CreationDateTime'])
            if 'EndDateTime' in cluster['Status']['Timeline']:
                print("    Ended:", cluster['Status']['Timeline']['EndDateTime'])
            print("NormalizedInstanceHours:", cluster['NormalizedInstanceHours'])
            print("")
    else:
        print("No clusters exists.")

