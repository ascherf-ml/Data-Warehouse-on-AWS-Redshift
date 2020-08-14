'''
Helper script to connect to the redshift cluster.

'''
import configparser
import psycopg2

def connect_redshift():
    """connects to redshift cluster
    
    Returns:
    conn: redshift connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT_CLUSTER'].values()))
    print('Connecting: ', conn)
    print('Connected to Redshift')
    return conn