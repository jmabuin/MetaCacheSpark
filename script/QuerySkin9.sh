#!/bin/bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 8G --driver-memory 5G MetaCacheSpark-0.0.2.jar -q -l 32 Database.parquet Output Genomica/inputs/skin9/SRR3407334.fasta