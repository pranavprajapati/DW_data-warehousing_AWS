import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
"""
Amazon Redshift does not support a single merge, or upsert, command to update a table from a single data source,. Hence we need to perform a merge operation by creating a staging table. This function loads the staging tables to update the target table from the staging table.
"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
"""
This function is used to insert values into the respective users,songs, artists, time tables. Values in songplay table are added from staging events and staging songs tables.
"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()