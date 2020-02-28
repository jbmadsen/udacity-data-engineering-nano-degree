import os
import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops the tables described by the drop_table_queries list of queries loaded from sql_queries.py
    
    Input:
    conn:   The connection to the database
    cur:    Cursor object created by the database connection
    """
    print("Dropping tables")

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables described by the create_table_queries list of queries loaded from sql_queries.py
    
    Input:
    conn:   The connection to the database
    cur:    Cursor object created by the database connection
    """
    print("Creating tables")

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Creates the database, and the staging, fact and dinmensional tables within the database"""
    config = configparser.ConfigParser()
    config.read('./dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to Redshift cluster")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Disconnected from Redshift cluster")


if __name__ == "__main__":

    # Set path to current directory
    os.chdir(os.path.dirname(sys.argv[0]))

    main()
