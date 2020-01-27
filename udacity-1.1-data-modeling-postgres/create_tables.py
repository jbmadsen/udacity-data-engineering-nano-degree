import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Drops the database 'sparkify' if it exists, and creates it again
    
    Returns:
    conn:The connection to the database
    cur:Cursor object created by the database connection
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops the tables described by the drop_table_queries list of queries loaded from sql_queries.py
    
    Input:
    conn:The connection to the database
    cur:Cursor object created by the database connection
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables described by the create_table_queries list of queries loaded from sql_queries.py
    
    Input:
    conn:The connection to the database
    cur:Cursor object created by the database connection
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Creates the database, and the fact and dinmensional tables within the database"""
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()