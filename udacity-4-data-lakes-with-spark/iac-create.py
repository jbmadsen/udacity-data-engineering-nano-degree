import boto3
import configparser


# Open and read the contents of the config file
ioc_config = configparser.ConfigParser()
ioc_config.read_file(open('./dl.cfg'))


# Load all the keys needed to create AWS services
KEY             = ioc_config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET          = ioc_config.get('AWS','AWS_SECRET_ACCESS_KEY')

DL_REGION       = ioc_config.get("DL","DWH_REGION")
DL_NUM_SLAVES   = ioc_config.get("DL","DL_NUM_SLAVES")
DL_NODE_TYPE    = ioc_config.get("DL","DL_NODE_TYPE")

S3_BUCKET_LOGS  = ioc_config.get("DL","S3_BUCKET")


# Creating resources/clients for infrastructure: EMR
connection = boto3.client('emr',
                          region_name=DL_REGION,
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,)


# Creating cluster settings: InstanceGroups
instance_groups = [
    {
        'Name': "Master",
        'Market': 'ON_DEMAND',
        'InstanceRole': 'MASTER',
        'InstanceType': DL_NODE_TYPE,
        'InstanceCount': 1,
    },
    {
        'Name': "Slave",
        'Market': 'ON_DEMAND',
        'InstanceRole': 'CORE',
        'InstanceType': DL_NODE_TYPE,
        'InstanceCount': DL_NUM_SLAVES,
    }
]

# Creating cluster
cluster_id = connection.run_job_flow(
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
    Steps=[
        {
            'Name': 'file-copy-step',   
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://Snapshot-jar-with-dependencies.jar',
                'Args': ['test.xml', 'emr-test', 'kula-emr-test-2']
            }
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    Tags=[
        {'Key': 'tag_name_1','Value': 'tab_value_1',},
        {'Key': 'tag_name_2','Value': 'tag_value_2',},
    ],
)


