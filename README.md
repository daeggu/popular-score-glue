# How to sort by average rating
 - **[Wilson scores](http://www.evanmiller.org/how-not-to-sort-by-average-rating.html)**
 - **[Laplace Smoothing](https://planspace.org/2014/08/17/how-to-sort-by-average-rating/)**
 - **[IMDB TOP 250 score](https://help.imdb.com/?ref_=helphdr_helphome)**
 - **[Reddit Story Ranking Algorithm](https://medium.com/hacking-and-gonzo/how-reddit-ranking-algorithms-work-ef111e33d0d9)**

# Laplace smoothing
> upvotes+α  / total votes+β

```
α and β are parameters that represent our estimation of
what rating is probably appropriate if we know nothing else.

If α = 1 and β = 2, post without vote is treated as 0.5 score.
Much easier to calculate than Wilson Score
```

# Popular Score
***AWS Glue*** is used to calculate the popular score by Laplace smoothing

## Job parameters
- --DEFAULT_SCORE   **0.35**<br>
- --REFLECTION_RATE **0.1** <br>
- --WAITING_TIME    **90**

## Job
### popular
- Start crawler to refresh the partition
- Get popular scores (ETL)
- Make fin(finish) file 

### waiting_to_retry_popular
 - Wait for the crawler to finish

## Triggers
### waiting_to_retry_popular (On-demand)
 - An exception is thrown when the crawler is already running. and then this trigger is triggered.

### retry_popular (Job event)
 - **retry-popular** is executed if **waiting_to_retry_popular** ends successfully.
 - Trigger the **popular** job.

## OOM Issue
Below is the complete information to overcome OOM issues in glue

Ideally there are 2 ways where the job can end up with OOM issues

Default glue runs on 5.5g executor memory including 512M Memory overhead each of the DPU’s will have 2 executor and out of the requested DPU’s one of the DPU’s is used as the Master with one executor.

### Driver running out of memory

If the file size ( Individual file size in s3  ) doesn't exceed HDFS block size, then  single file will be assigned to each of the tasks inside the executor. By default, each executor uses 1 core, which means each executor will run 1 task at a time, if there are many such small files, then each file state is maintained on Application master which may result in driver running out of memory.

To over come this there are two ways.

- To increase the driver memory

      1. Open Glue> Jobs > Edit your Job> Job parameters near the bottom
      2. Set the following:
            key: --conf
            value: spark.driver.memory=10g

- Glue group by options  which groups the files to the specified number of bytes  before it processes it, this grouped files are assigned to the task rather than individual file.
Example syntax

df_catalog = glueContext.create_dynamic_frame_from_catalog(database = "etltest", table_name = "cylance_threat_v2, additional_options={'groupFiles': 'inPartition', 'groupSize': '1024000'})

### Executor running out of memory

Usually executor runs out of memory when process the large data like in your case, also when the input format os not splittable 
To over come the executor running out of the memory you can set the spark.yarn.executor.memoryOverhead to max of 7168MB
On setting the spark memory over head there are few trade off that needs to be considered.

* Setting spark.yarn.executor.memoryOverhead to 1024, will give each executor 6.5 Gs of memory, also each DPU will have 2 executors running in parallel.
* Setting spark.yarn.executor.memoryOverhead to anything above 1024MB say max value 7168MB then entire nodes memory will be allocated to a single executor, Note that here each of the DPU’s will have single executor ( Affecting the parallelism).

You can do the following to see how the job behaves.

1. Increase the driver memory to Max i.e 10g and executor memory to 1024 ( if this fails, thinking the amount of DATA please increase the executor memory to max of 7168), which be done by the following step

    	1. Open Glue> Jobs > Edit your Job> Job parameters near the bottom
    	2. Set the following:
      		   key: --conf
		          value: spark.driver.memory=10g --conf spark.yarn.executor.memoryOverhead=1024