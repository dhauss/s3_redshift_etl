"""Drop any existing tables, then create tables defined in sql_queries.py."""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all existing staging, fact, and dimension tables.

    Args:
        conn: (connection) instance of connection class
        cur: (cursor) instance of cursor class

    Returns:
        none

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging, fact, and dimension tables.

    Args:
        conn: (connection) instance of connection class
        cur: (cursor) instance of cursor class

    Returns:
        none

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Parse config file and establish connection with Postgres DB, call functions and close connection."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()