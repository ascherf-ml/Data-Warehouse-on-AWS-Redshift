import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from connect_to_redshift import connect_redshift


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        


def main():
    
    conn = connect_redshift()
    cur = conn.cursor()

    print('Dropping Tables')
    drop_tables(cur, conn)
    print('Tables dropped successfully')
    print('Creating Tables')
    create_tables(cur, conn)
    print('Tables are created successfully.')

    conn.close()


if __name__ == "__main__":
    main()