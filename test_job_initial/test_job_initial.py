import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
import pytz
import pymysql.cursors
from contextlib import closing

#https://ap-south-1.console.aws.amazon.com/gluestudio/home?region=ap-south-1#/editor/job/Spark-PTC-CommonContractData/runs

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_cntdata_log',
    'mySQLport',
    'mySQLusername',
    'mySQLpassword',
    'mySQLdb',
    'mySQLtable',
    'mySQLtablestatus',
    'mySQLtablestaging'
])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

ist = pytz.timezone('Asia/Calcutta')
dt_current_day = (datetime.now(ist))
dt_prev_day = (datetime.now(ist)-timedelta(1)).strftime("%Y-%m-%d")
hostName = "trades-and-charges-uat.cluster-c0iswfmnnzar.ap-south-1.rds.amazonaws.com"

def write_df(df, url, table, username, password):
    df.write.mode("append") \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.mysql.jdbc.Driver") \
    .save()

def get_s3_cntdata_log_from_s3(s3_path):
    # if no data is populated from backoffice to s3, log and raise exception to fail the job.
    current_date = dt_current_day.strftime("%Y-%m-%d")
    s3_path = f"{s3_path}dt={current_date}/last1day"
    logger.info(f"cntdatalog s3 location : {s3_path}")
    
    df = spark.read.orc(s3_path)
    if df.count() == 0:
        raise Exception(f"cntdatalog is not populated from backoffice to s3: {s3_path}")

    df = df.selectExpr("cast(CNT_DATE as timestamp)cnt_date",
                    "TRIM(CLTCODE)cltcode",
                    "TRIM(GLCODE)glcode",
                    "TRIM(GSTCODE)gstcode",
                    "cast(CHARGAMT as decimal(18,4))chargamt",
                    "cast(GST_CHARGES as decimal(18,4))gst_charges",
                    "TRIM(NARRATION)narration",
                    "TRIM(BRANCH)branch",
                    "TRIM(PRODUCT)product",
                    "TRIM(EXCHANGE)exchange",
                    "cast(RUNTIME as timestamp)runtime",
                )

    df = df.na.fill(value=0)
    df = df.cache()
    return df
    
def insert_cntdata_log_in_RDS(host, database, user, password,port, table, table_stg):
    str_delete_prevdata_q = f"DELETE FROM {table} WHERE cnt_date>= '{dt_prev_day}' and cnt_date< '{dt_current_day}'"   
    insert_query = f"insert into {table} select * from {table_stg}"
    connection = getDatabaseConnection(host, user, password, database)
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(str_delete_prevdata_q)
            logger.info("deleted previous days data successfully from database")
            cursor.execute(insert_query)
            logger.info("inserted cntdata_log data successfully in database")
        connection.commit()

def getDatabaseConnection(host, user, password, database): 
    connection = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             database=database,
                             cursorclass=pymysql.cursors.DictCursor)
    return connection
    
def execute_query(query, database, host, port, user, password):
    connection = getDatabaseConnection(host, user, password, database)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()

def update_job_status(host, database, user, password, port, table, table_status ):
    update_status_query = '''insert into {0}(job_name, data_updated_till, last_success_run)
                            values('{1}', (select max(cnt_date) from {2}), '{3}')
                            ON DUPLICATE Key UPDATE
                                data_updated_till=(select max(cnt_date) from {2}),
                                last_success_run='{3}' '''.format(table_status, args['JOB_NAME'], table, dt_current_day)

    execute_query(query=update_status_query, host=host, database=database, password=password, user=user, port=port)

def execute_database_operations(df_cntdata_log, host, port, database, table_stg, table, user, password, table_status):
    # Truncate staging table in single transaction.
    truncate_stg_tbl_query = f"truncate table {table_stg}"
    execute_query(query=truncate_stg_tbl_query, host=host, database=database, password=password, user=user, port=port)
    logger.info("truncated staging table successfully")

    # write data on Staging Table
    mySQLurl = f'''jdbc:mysql://{host}:{port}/{database}?connectTimeout=5000&socketTimeout=300000&enabledTLSProtocols=TLSv1.2'''
    logger.info(f"mySQLurl : {mySQLurl}")
    
    # populate data in staging table.
    write_df(df_cntdata_log, mySQLurl, table_stg, user, password)
    logger.info("data populated in staging table successfully")

    # insert data into main table
    insert_cntdata_log_in_RDS(host=host, database=database, user=user, password=password, port=port, table=table, table_stg=table_stg)
    logger.info("inserted in main table successfully")

    update_job_status(host=host, database=database, user=user, password=password, port=port, table=table, table_status=table_status )
    logger.info("updated job run status successfully")

    
def process_cntdata_log():
    try: 
        mySQLport = args['mySQLport']
        mySQLusername = args['mySQLusername']
        mySQLpassword = args['mySQLpassword']
        mySQLdb = args['mySQLdb']
        mySQLtablestatus = args['mySQLtablestatus']
        mySQLtablestaging = args['mySQLtablestaging']
        mySQLtable = args['mySQLtable']
        s3_cntdata_log=args['s3_cntdata_log']
        df_cntdata_log = get_s3_cntdata_log_from_s3(s3_cntdata_log)

        # excute database operation for main table
        execute_database_operations(df_cntdata_log, host=hostName, port=mySQLport, database=mySQLdb,
                                    table_stg=mySQLtablestaging, table=mySQLtable, user=mySQLusername, password=mySQLpassword, table_status=mySQLtablestatus)

        logger.info("db operations for RDS executed successfully")

        # unpersist all df
        spark.catalog.clearCache()
        
    except Exception as e:
        logger.error("failed to process cntdatalog")
        raise e

if __name__ == "__main__":
    process_cntdata_log()

job.commit()
