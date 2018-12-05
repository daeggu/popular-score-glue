import sys
import time
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WAITING_TIME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print 'WAITING_TIME : ', args['WAITING_TIME']
print 'waiting to finish [raw_crawler]'
time.sleep(float(args['WAITING_TIME']))
print 'retry [popular] JOB'

job.commit()