'''
Helper script to check the status of the redshift cluster.

'''

import boto3
import json
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read('dwh.cfg')

'''
    Builds a connection to the AWS services.
    
'''
redshift = boto3.client('redshift',
            aws_access_key_id=config.get('AWS','KEY'),
            aws_secret_access_key=config.get('AWS','SECRET'),
            region_name=config.get('AWS','AWS_REGION')
            )
    
try:
    cluster_status = redshift.describe_clusters(
    ClusterIdentifier=config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER')
        )
except Exception as e:
    print('could not get cluster status', e)
    
redshift_cluster = cluster_status['Clusters'][0]
print(redshift_cluster)

print("Redshift endpoint :: ", redshift_cluster['Endpoint']['Address'])
print("IAM role ARN :: ", redshift_cluster['IamRoles'][0]['IamRoleArn'])
