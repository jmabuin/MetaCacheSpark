#!/bin/bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --conf spark.yarn.driver.memoryOverhead=1500 --executor-memory 8G --driver-memory 7G MetaCacheSpark-0.3.0.jar -q -u -l 32 DatabaseNative OutputhiSeq Genomica/inputs/HiSeq_timing.fa