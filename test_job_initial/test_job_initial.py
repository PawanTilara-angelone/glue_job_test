import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
import pytz
from datetime import datetime ,timedelta
from contextlib import closing

#job link:- https://ap-south-1.console.aws.amazon.com/gluestudio/home?region=ap-south-1#/editor/job/data_migration_from_timeseries_to_ADB/details

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

timeseries_host_uat = '192.168.144.19'
timeseries_password = 'Trade123#Timeseries'
timeseries_port = '5432'
timeseries_driver = 'org.postgresql.Driver'
timeseries_username = 'trade'
timeseries_database = 'timeseries'
mySQLhost = "trades-and-charges-uat.cluster-c0iswfmnnzar.ap-south-1.rds.amazonaws.com"
mySQLport = "3306"
mySQLusername = "spark_post_trade_uat"
mySQLpassword = "fw02gdmqOVYGnvEy3QmV"
mySQLdb = "spark-trades-and-charges"
mySQLdriver = "com.mysql.jdbc.Driver"

ist = pytz.timezone('Asia/Calcutta')
startDate = '2022-01-01 00:00:00.000'
endDate = '2022-05-01 00:00:00.000'

mySQLurl = f'''jdbc:mysql://{mySQLhost}:{mySQLport}/{mySQLdb}?connectTimeout=5000&socketTimeout=300000&enabledTLSProtocols=TLSv1.2'''

def write_df(df, url, driver, table, username, password):
    try:
        df.write.mode("append") \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "vw_transaction_subquery") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .save()
    except Exception as e:
        logger.error("failed to write spark df for url: {0}, table: {1}", url, table)
        raise e

def migrateSettlementData():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    settlementQuery = '''
            SELECT sauda_date, party_code, segment, order_no, trade_no, stt, stampduty, trade_type
            FROM settlement
            WHERE sauda_date>={0} AND sauda_date<{1}
        '''.format(startDate, endDate)
    logger.info(f"reading data from main : Query {settlementQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",settlementQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("settlement dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for settlement, date range {startDate} - {endDate}")
    
def migrateNonTradeChargesSegmentwiseData():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    nonTradeChargesSegmentwiseQuery = '''
            SELECT transdate, party_code, segment, margin_shortfall_penalty, callntrade_charges, physical_cn_handling_charges, updated_at
            FROM tbl_nontrade_charges_segmentwise
            WHERE transdate>={0} AND transdate<{1}
        '''.format(startDate, endDate)
    logger.info(f"reading data from main : Query {nonTradeChargesSegmentwiseQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",nonTradeChargesSegmentwiseQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("NonTradeChargesSegmentwise dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for nonTradeChargesSegmentwise, date range {startDate} - {endDate}")
    
def migrateNonTradeChargesData():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    nonTradeChargesQuery = '''
            SELECT transdate, party_code, dp_charges, delayed_payment_charges, demat_remat_charges, pledge_charges, unpledge_charges, monthly_account_maintanance, date_created, date_updated
            FROM tbl_nontrade_charges
            WHERE transdate>={0} AND transdate<{1}          
        '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {nonTradeChargesQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",nonTradeChargesQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("NonTradeCharges dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for NonTradeCharges, date range {startDate} - {endDate}")
    
def migrateCommonContractData():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    commonContractQuery = '''
            SELECT sauda_date, party_code, exchange, segment, scripname, isin, order_no, trade_no, qty, trade_type, buysell, brokerage, service_tax,ins_chrg,
            sebi_tax, turn_tax, broker_chrg, other_chrg, market_rate, market_amt
            FROM common_contract_data
            WHERE sauda_date>={0} AND sauda_date<{1}          
        '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {commonContractQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",commonContractQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("CommonContract dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for CommonContract, date range {startDate} - {endDate}")
    
def migrateCliInterestData():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    cliInterestQuery = '''
            SELECT branch_cd, sub_broker, party_code, party_name, balancedate, bsecm, nsecm, bsefo, nsefo, mcx, ncdex, total, interestrate, interestamount, updatedon,
            brokerage, mcd, nsx, acbpl_total, acbpl_intamt, cgst, sgst, igst, ugst, clistate
            FROM cliinterest
            WHERE balancedate>={0} AND balancedate<{1}          
        '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {cliInterestQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",cliInterestQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("cliInterest dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for cliInterest, date range {startDate} - {endDate}")
    
def migrateVWTransactionSubquery():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    vwTransactionQuery = '''
            SELECT clic_trans_dt, dpam_bbo_code, isin, scriptcode, narration, clic_charge_amt, gst, totalhead, head, cdshm_dpam_id, cdshm_internal_trastm
            FROM vw_transaction_subquery
            WHERE clic_trans_dt>='{0}' AND clic_trans_dt<'{1}'          
        '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {vwTransactionQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("query",vwTransactionQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("vwTransactionSubquery dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for vwTransactionSubquery, date range {startDate} - {endDate}")
      
def migrateDeliveryDPchargesFinalAmount():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    deliverydpchargesfinalamountQuery = '''
            SELECT party_code, scrip_cd, series, qty, transdate, charge_scope, charge_type, depos_fixed_charges, flat_charge, min_charge, percentage_charge,scrip_closing_rate, totalcharges
            FROM deliverydpchargesfinalamount
            WHERE transdate>='{0}' AND transdate<'{1}'          
        '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {deliverydpchargesfinalamountQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("header", "True")\
                        .option("query",deliverydpchargesfinalamountQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
                        
    logger.info("deliverydpchargesfinalamount dataframe read from primary server successfully for date range")
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for deliverydpchargesfinalamount, date range {startDate} - {endDate}")
    
def migrateCntDataLog():
    timeseries_url = f"jdbc:postgresql://{timeseries_host_uat}:{timeseries_port}/timeseries"
    logger.info("url is" + timeseries_url)
    cntDataLogQuery = '''
            SELECT cnt_date, cltcode, glcode, gstcode, chargamt, gst_charges, narration, branch, product, exchange, runtime
            FROM tbl_cntdata_log
            WHERE cnt_date>='{0}' AND cnt_date<'{1}' '''.format(startDate, endDate) 
    logger.info(f"reading data from main : Query {cntDataLogQuery}")
    df = spark.read.format("jdbc") \
                        .option("url",timeseries_url) \
                        .option("driver", timeseries_driver) \
                        .option("header", "True")\
                        .option("query",cntDataLogQuery) \
                        .option("user",timeseries_username) \
                        .option("password",timeseries_password) \
                        .load()
    
    # if df.count() == 0:
    #     logger.info("count is zero")
    # logger.info(df.count())
    df.printSchema()
    df.show(n=10, truncate=False, vertical=False)
    df = df.selectExpr("cast(cnt_date as timestamp)cnt_date",
                    "cast(cltcode as string)cltcode",
                    "cast(glcode as string)glcode",
                    "cast(gstcode as string)gstcode",
                    "cast(chargamt as decimal(18,4))chargamt",
                    "cast(gst_charges as decimal(18,4))gst_charges",
                    "cast(narration as string)narration",
                    "cast(branch as string)branch",
                    "cast(product as string)product",
                    "cast(exchange as string)exchange",
                    "cast(runtime as timestamp)runtime",
                )
    df.show(n=10, truncate=False, vertical=False)
    df.printSchema()
    logger.info("cntDataLog dataframe read from primary server successfully for date range")
    logger.info("url is " + mySQLurl)
    write_df(df, mySQLurl, mySQLdriver, mySQLdb, mySQLusername, mySQLpassword)
    logger.info(f"successfully migrated data for cntDataLog, date range {startDate} - {endDate}")
    
def processDataMigrationData():
    #will be calling these functions one by one to migrate data from timeseries to aroura db
    # migrateSettlementData()
    # migrateNonTradeChargesSegmentwiseData()
    # migrateNonTradeChargesData()
    # migrateCommonContractData()
    # migrateCliInterestData()
    migrateVWTransactionSubquery()
    # migrateDeliveryDPchargesFinalAmount()
    # migrateCntDataLog()
    
if __name__ == "__main__":
    processDataMigrationData()

job.commit()
