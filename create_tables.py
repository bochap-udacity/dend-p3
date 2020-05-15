import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import aws

def drop_tables(cur, conn):
    """
        Get and runs all queries to DROP all objects in the Redshift instance
    """
    for query in drop_table_queries:
        print(f"Running: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Get and runs all queries to CREATE all objects in the Redshift instance
    """    
    for query in create_table_queries:
        print(f"Running: {query}")
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read([
        "dwh.cfg",
        "secrets.cfg"
    ])

    host = aws.get_host()
    print("Connecting to Redshift")
    conn = psycopg2.connect(
        "host={} dbname={} port={} user={} password={}".format(
            host, *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    print("Dropping tables")
    drop_tables(cur, conn)
    print("Creating tables")
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()