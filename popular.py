import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import boto3
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WAITING_TIME', 'REFLECTION_RATE', 'DEFAULT_SCORE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Trigger Crawler before Step
print '[Before ETL]'
print 'Start crawler!!'
glue_client = boto3.client('glue', region_name='ap-northeast-1')

try : 
    res = glue_client.start_crawler(Name='raw_crawler')
    print 'End crawler : ', res
    time.sleep(float(args['WAITING_TIME']))
except Exception as e : 
    print 'Exception : %s' % e.message
    print 'Start trigger [waiting_to_retry_popular] '
    glue_client.start_trigger(Name = 'waiting_to_retry_popular')
    raise Exception("'raw_cralwer' has already been executed. It will be running again after a certain period of time.")

# Time Setting
print '[Step0] Time & Predicate Setting'
now = datetime.now()
a_week_ago = now - timedelta(days=7)
print 'now : ', now
print 'a_week_ago : ', a_week_ago

# Data Catalog: database and table name
db_name = "bh_ml"
tbl_name = "raw"
bucket_name = 'm1-prd-ml-bucket'
fin_path = 'popular/%s/%s/%s/' % (now.year, now.month, now.day)
filename = '%s' % (now.strftime('%H%M%S'))

output_path = 's3://%s/popular/%s/%s/%s/%s/' % (bucket_name, now.year, now.month, now.day, now.strftime('%H%M%S'))
#partitionPredicate = 'year = %s AND month = %s AND day BETWEEN %s AND %s' % (now.year, now.month, a_week_ago.day, now.day)
partitionPredicate = "to_date(concat(year,'-', month, '-', day)) >=  to_date('%s')" % a_week_ago.strftime('%Y-%m-%d')

print 'fin_path : ', fin_path
print 'filename : ', filename
print 'output path : ', output_path
print 'predicate : ', partitionPredicate


## @type: DataSource
## @args: [database = db_name, table_name = tbl_name]
## @return: scored_data
## @inputs: []
scored_data = glueContext.create_dynamic_frame.from_catalog(
    database = db_name,
    table_name = tbl_name,
    push_down_predicate = partitionPredicate)
    
print '[Step1] Popular ETL Start'
print 'scored_data count : ', scored_data.count()
print 'scored_data schema : '
scored_data.toDF().printSchema()
print 'scored_data show : '
scored_data.toDF().show()

## @type: RemoveHeader
## @args: []
## @return: scored_df
## @inputs: [frame = scored_data]
print '[Step2] chage to DataFrame & (remove header)'
scored_df = scored_data.toDF()
#scored_df = scored_df.where(scored_df.deviceid != 'deviceid')
print 'scored_df count : ', scored_df.count()

## @type: Get_TotalVotes
## @args: []
## @return: total_votes_count_df
## @inputs: [frame = scored_df]
print '[Step3] get total_votes'
feature_group = ['country_code', 'loc_week', 'loc_hour', 'card']
total_votes_count_df = scored_df.groupBy(feature_group).count().withColumnRenamed("count", "total_votes")
print 'total_votes_count_df count : ', total_votes_count_df.count()
print 'total_votes_count_df show : '
total_votes_count_df.show(20)


## @type: Get_UpVotes
## @args: []
## @return: up_votes_count_df
## @inputs: [frame = total_votes_count_df]
print '[Step4] get up_votes'
up_votes_count_df = scored_df.filter(scored_df.score >= 4.0).groupBy(feature_group).count().withColumnRenamed("count", "up_votes")
print 'up_votes_count_df count : ', up_votes_count_df.count()
print 'up_votes_count_df show : '
up_votes_count_df.show(20)


## @type: JoinTable
## @args: []
## @return: temp
## @inputs: [frame = up_votes_count_df]
print '[Step5] joined Table (total_votes, upvotes)'
temp_df = total_votes_count_df.join(up_votes_count_df, feature_group, "left_outer").na.fill({'up_votes': 0})
temp = DynamicFrame.fromDF(temp_df, glueContext, "temp")
print 'temp_df count : ', temp_df.count()
print 'temp_df show : '
temp_df.show(20)


## @type: Laplace
## @args: []
## @return: laplace
## @inputs: [frame = temp]
print '[Step6] calculate Laplace Smoothing'
votes_avr = total_votes_count_df.select(mean("total_votes")).collect()[0]['avg(total_votes)']
reflection_rate = float(args['REFLECTION_RATE'])
default_score = float(args['DEFAULT_SCORE'])
alpha = int(votes_avr * reflection_rate * default_score)
beta = int(votes_avr * reflection_rate)
print 'votes_avr : ', votes_avr
print 'alpha, beta', alpha, beta

def getScore(temp) :
    try:
        score = (float(temp['up_votes']) + alpha) / (float(temp['total_votes']) + beta)
        temp['laplace'] = score
        return temp
    except Exception as ex:
        print 'getScore(temp) func error : ', ex
        temp['laplace'] = default_score
        return temp;

laplace = Map.apply(frame = temp, f =  getScore)
print 'laplace schema : ',
laplace.toDF().printSchema()
print 'laplace show '
laplace.toDF().show(20)


## @type: ApplyMapping
## @args: []
## @return: final
## @inputs: [frame = laplace]
print '[Step7] ApplyMapping'
final = ApplyMapping.apply(frame = laplace, 
    mappings = [("country_code", "string", "country", "string"),
    ("loc_week", "int", "loc_week", "int"), 
    ("loc_hour", "int", "loc_hour", "int"), 
    ("card", "string", "card", "string"), 
    ("laplace", "double", "score", "float")])
print 'final show :'
final.toDF().show()


## @type: DataSink
## @args: []
## @return: datasink
## @inputs: [frame = final]
datasink = glueContext.write_dynamic_frame.from_options(frame = final, connection_type = "s3", connection_options = {"path": output_path}, format = "csv")

print '[Step8] Make fin file'
s3 = boto3.resource('s3')

try : 
    object = s3.Object(bucket_name, fin_path + filename + '.fin')
    object.put(Body='')
except Exception as e:
    print('Exception: %s' % e.message)

job.commit()