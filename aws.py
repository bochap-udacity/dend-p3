import boto3
from configparser import SafeConfigParser
import json
from timeit import default_timer as timer

config = SafeConfigParser()
config.read([
    "dwh.cfg",
    "secrets.cfg"
])

aws_key = config.get('AWS','KEY')
aws_secret = config.get('AWS','SECRET')
aws_region = config.get('AWS','REGION')
aws_iam_role = config.get('AWS','IAM_ROLE')
cluster_identifier = config.get('CLUSTER','IDENTIFIER')
cluster_type = config.get('CLUSTER','TYPE')
cluster_num_nodes = config.get('CLUSTER','NUM_NODES')   
cluster_node_type = config.get('CLUSTER','NODE_TYPE')
cluster_db_name = config.get('CLUSTER','DB_NAME')
cluster_db_port = config.get('CLUSTER','DB_PORT')
cluster_db_user = config.get('CLUSTER','DB_USER')  
cluster_db_password = config.get('CLUSTER','DB_PASSWORD')  

ec2 = boto3.resource(
    "ec2", region_name=aws_region,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

iam = boto3.client(
    "iam",
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

redshift = boto3.client(
    "redshift", region_name=aws_region,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

def teardown():
  """
    Removes all the AWS resource created to support this project
  """
  print()
  print(f"Starting AWS environment teardown")
  print("#" * 100)  
  try:
    print()
    print(f"Starting Redshift cluster {cluster_identifier} clean up")
    print("-" * 100)    

    cluster_description = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    print(f"Remove TCP public access Redshift cluster {cluster_identifier}")
    vpc = ec2.Vpc(id=cluster_description['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.revoke_ingress(
        GroupName=defaultSg.group_name, 
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(cluster_db_port),
        ToPort=int(cluster_db_port)
    )
    print(f"Delete Redshift cluster {cluster_identifier}")
    redshift.delete_cluster( ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
    start = timer()
    while cluster_description != None:
      time_taken = (timer() - start)
      print(f"Waiting {int(time_taken//60):02}m {int(time_taken%60):02}s", end="\r")
      cluster_description = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
  except Exception as e:
    print(f"Completed Redshift cluster {cluster_identifier} clean up")

  try:
    print()
    print(f"Starting IAM Role {aws_iam_role} clean up")
    print("-" * 100)
    print(f"Removing S3 Readonly access from IAM Role {aws_iam_role}")
    iam.detach_role_policy(RoleName=aws_iam_role, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print(f"Deleting IAM Role {aws_iam_role}")
    iam.delete_role(RoleName=aws_iam_role)
    print(f"Completed IAM Role {aws_iam_role} clean up")
  except Exception as e:
    print(f"*** Error: {e}")

  print("#" * 100)
  print(f"Completed AWS environment teardown")

def setup():
  """
    Create all the AWS resource that are created to support this project
  """  
  print()
  print(f"Starting AWS environment setup")
  print("#" * 100)
  try:
    print()
    print(f"Starting IAM Role {aws_iam_role} creation")
    print("-" * 100)
    print(f"Creating IAM Role {aws_iam_role}")
    iam.create_role(
      Path="/",
      RoleName=aws_iam_role,
      Description="Allows Redshift to have readonly access to AWS S3 for Udacity DEND",
      AssumeRolePolicyDocument=json.dumps({
        "Statement":[{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            }
        }],
        "Version": "2012-10-17"        
      })
    )

    print(f"Attaching S3 Readonly access to IAM Role {aws_iam_role}")
    iam.attach_role_policy(
        RoleName=aws_iam_role,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )["ResponseMetadata"]["HTTPStatusCode"]    
    print(f"Completed IAM Role {aws_iam_role} creation")
    
    print()
    print(f"Starting Redshift cluster {cluster_identifier} creation")
    print("-" * 100)
    print(f"Get IAM Role {aws_iam_role}")  
    role_arn = iam.get_role(RoleName=aws_iam_role)["Role"]["Arn"]
    print(f"Creating Redshift cluster {cluster_identifier}")
    redshift.create_cluster(        
        ClusterIdentifier=cluster_identifier,
        ClusterType=cluster_type,
        NodeType=cluster_node_type,
        NumberOfNodes=int(cluster_num_nodes),
        DBName=cluster_db_name,
        Port=int(cluster_db_port),
        MasterUsername=cluster_db_user,
        MasterUserPassword=cluster_db_password,
        IamRoles=[role_arn]
    )    
    cluster_description = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    start = timer()
    while cluster_description["ClusterStatus"] != "available":
      time_taken = (timer() - start)
      print(f"Waiting {int(time_taken//60):02}m {int(time_taken%60):02}s", end="\r")
      cluster_description = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]

    print(f"Open TCP public access Redshift cluster {cluster_identifier}")
    vpc = ec2.Vpc(id=cluster_description['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name, 
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(cluster_db_port),
        ToPort=int(cluster_db_port)
    )

    print(f"Completed Redshift cluster {cluster_identifier} creation in {time_taken//60}m {int(time_taken%60)}s")
  except Exception as e:
    print(f"*** Error: {e}")
  
  print("#" * 100)
  print(f"Completed AWS environment setup")

def get_host():
  """
    Get address to allow access into the Redshift instance
  """
  return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]["Endpoint"]["Address"]

def get_iam_role_arn():
  """
    Get IAM role created for this project
  """
  return iam.get_role(RoleName=aws_iam_role)["Role"]["Arn"]