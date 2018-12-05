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