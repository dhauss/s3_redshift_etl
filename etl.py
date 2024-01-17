"""Populate staging, fact, and dimension tables."""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Populate staging tables using data from S3.

    Args:
        conn: (connection) instance of connection class
        cur: (cursor) instance of cursor class

    Returns:
        none

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populate fact and dimension tables using staging tables.

    Args:
        conn: (connection) instance of connection class
        cur: (cursor) instance of cursor class

    Returns:
        none

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Parse config file and establish connection with Postgres DB, call functions, close connection."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()