import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    #copy tables
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
        #insert tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to the cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()