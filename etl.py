import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Executes COPY commands that loads data from S3 bucket into staging tables in Redhsift cluster"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Executes INSERT statements that transform and load data from the staging tables into Fact and Dimension tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Load Redshift cluster parameters from config file"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """ Create a connection to the Redshift cluster"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """ Execute functions to load data into staging tables, transform and insert into Fact and Dimension tables"""
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    """ Close connection to Redshift cluster"""
    conn.close()


if __name__ == "__main__":
    main()