import os
import sys
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Imports S3 data into staging tables described by the copy_table_queries list of queries loaded from sql_queries.py
    
    Input:
    cur:    Cursor object created by the database connection
    conn:   The connection to the database
    """
    print("Loading into staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into tables described by the insert_table_queries list of queries loaded from sql_queries.py
    
    Input:
    cur:    Cursor object created by the database connection
    conn:   The connection to the database
    """
    print("Inserting into tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Loads data from S3 into staging tables, then transforms data and loads this into Fact and dimensional tables"""
    config = configparser.ConfigParser()
    config.read('./dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to Redshift cluster")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print("Disconnected from Redshift cluster")


if __name__ == "__main__":
    
    # Set path to current directory
    os.chdir(os.path.dirname(sys.argv[0]))
    
    main()
