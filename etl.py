import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import aws

config = configparser.ConfigParser()
config.read(["dwh.cfg", "secrets.cfg"])
aws_region = config.get('AWS','REGION')
s3_log_data = config.get('S3','LOG_DATA')
s3_log_jsonpath = config.get('S3','LOG_JSONPATH')
s3_song_data = config.get('S3','SONG_DATA')
host = aws.get_host()
aws_iam_role_arn = aws.get_iam_role_arn()

def load_staging_tables(cur, conn):
    """
        Load raw data into the staging tables
    """        
    for query in copy_table_queries(aws_iam_role_arn, aws_region, s3_log_data, s3_log_jsonpath, s3_song_data):
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
        Insert raw data into the fact and dimension tables
    """      
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    conn = psycopg2.connect(
        "host={} dbname={} port={} user={} password={}".format(
            host, *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
