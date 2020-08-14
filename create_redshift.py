'''
Helper script to create an Amazon IAM user and start and configurate a redshift cluster.

'''

import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

'''
The create_iam function takes information based in the config variable (created at the main function).
- First the AWS are called with a KEY and a SECRET.
- Then an IAM role is created and the ARN is saved.
- If there is already an existing IAM role the already created ARN is returned.

'''
def create_iam(config):
    iam_role_name = config.get('IAM_ROLE', 'NAME')
    
    iam = boto3.client('iam',
                   aws_access_key_id=config.get('AWS','KEY'),
                   aws_secret_access_key=config.get('AWS','SECRET'),
                   region_name=config.get('AWS','AWS_REGION')
                  )

    
    try:
        iamrole = iam.create_role(
        RoleName=iam_role_name,
        Description = "Allows Redshift clusters to call AWS services.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
             'Effect': 'Allow',
             'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
        )
        
    except iam.exceptions.EntityAlreadyExistsException:
        print('There is a IAM role already.')
        iamrole = iam.get_role(RoleName=iam_role_name)
        print('IAM Role Arn code:', iamrole['Role']['Arn'])
        
    try:
        iam.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )
    except Exception as e:
        raise e
    print('IAM role created or exists already!')
    
    return iamrole



'''
The create_redshift_cluster function creates a redshift instance on AWS with the parameters from
config and the created (or pulled) IAM user role.

- First a connection is made with AWS given parameters from the config variable.
- Then a redshift cluster is created and the IAM role (created by the create_iam function) is connected to it.

'''
def create_redshift_cluster(config, iam_role):
    redshift = boto3.client('redshift',
                        aws_access_key_id=config.get('AWS','KEY'),
                        aws_secret_access_key=config.get('AWS','SECRET'),
                        region_name=config.get('AWS','AWS_REGION')
                       )
    
    try:
        response = redshift.create_cluster(  
        DBName=config.get('REDSHIFT_CLUSTER', 'DB_NAME'),
        ClusterIdentifier=config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER'),
        MasterUsername=config.get('REDSHIFT_CLUSTER', 'DB_USER'),
        MasterUserPassword=config.get('REDSHIFT_CLUSTER', 'DB_PASSWORD'),
        
        ClusterType=config.get('REDSHIFT_CLUSTER', 'CLUSTER_TYPE'),
        NodeType=config.get('REDSHIFT_CLUSTER', 'NODE_TYPE'),
        Port=int(config.get('REDSHIFT_CLUSTER', 'DB_PORT')),
        NumberOfNodes=int(config.get('REDSHIFT_CLUSTER', 'NODE_COUNT')),

        IamRoles=[iam_role['Role']['Arn']]  
        )
        print('Creating redshift cluster.')
    except Exception as e:
        print(e)

'''
The main function creates the config variable from the dwh.cfg file and connects it to the create_iam and
create_redshift_cluster functions.

'''
def main():
    """
    - loads in the config
    - creates iam role
    - creates redshift cluster
    - waits for redshift cluster to be created, and displays details
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    iam_role = create_iam(config)
    create_redshift_cluster(config, iam_role)
    


if __name__ == '__main__':
    main()