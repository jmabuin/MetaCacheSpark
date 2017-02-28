#!/bin/bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 5G --driver-memory 5G MetaCacheSpark-0.0.2.jar -q -l 32 -r -a Database.parquet Output Genomica/inputs/skin9/inputSmall.fasta