'''
Helper script to detach and delete the IAM user and finaly delete the redshift cluster.

'''

import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError


def delete_iam(config):
    '''
    Builds a connection to the AWS services.
    
    '''
    iam = boto3.client('iam',
        aws_access_key_id=config.get('AWS','KEY'),
        aws_secret_access_key=config.get('AWS','SECRET'),
        region_name=config.get('AWS','AWS_REGION')
        )
    
    try:
        iam.detach_role_policy(RoleName=config.get('IAM_ROLE', 'NAME'),
                               PolicyArn=config.get('IAM_ROLE', 'ARN'))
        print('Detaching IAM role policy.')
    except Exception as e:
        print('Could not detach IAM role policy!', e)
        
    try:
        iam.delete_role(RoleName=config.get('IAM_ROLE', 'NAME'))
        print('Deleting IAM role.')
    except Exception as e:
        print('Could not delete IAM role!', e)

def delete_redshift_cluster(config):
    '''
    Builds a connection to the AWS services.
    
    '''
    redshift = boto3.client('redshift',
        aws_access_key_id=config.get('AWS','KEY'),
        aws_secret_access_key=config.get('AWS','SECRET'),
        region_name=config.get('AWS','AWS_REGION')
        )
    
    '''
    Deleting the cluster, given the access information.
    '''    
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
        print('Deleting redshift cluster.')
        
    except Exception as e:
        print('Could not delete redshift cluster.', e)


        
def main():
    """
    The main function should get the config and
    then, remove the iam role and policy if they exist.
    Then it will delete the existing redshift cluster.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    delete_iam(config)
    delete_redshift_cluster(config)

if __name__ == '__main__':
    main()