import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Execute DROP statements for each table to be created """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Create staging, fact and dimension tables """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Load Redshift cluster parameters from config file"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """Connect to Redshift cluster"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """ Begin to execute queries on clusters """
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    """ Close connection to cluster """
    conn.close()


if __name__ == "__main__":
    main()