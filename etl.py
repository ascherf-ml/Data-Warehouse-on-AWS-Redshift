import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from connect_to_redshift import connect_redshift

'''
Loading the staging tables defined in the sql_queries file. 
    
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Now loading: \n', query)
        cur.execute(query)
        conn.commit()
        
'''
Inserting data into the fact and dimension tables fro mthe staging tables; defined in the sql_queries file. 
    
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Now inserting: \n', query)
        cur.execute(query)
        conn.commit()
        


def main():
    '''
    Builds a connection to the AWS services via the helper file connect_to_redshift.
    
    '''
    conn = connect_redshift()
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()