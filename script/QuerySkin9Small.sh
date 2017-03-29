#!/bin/bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 8G --driver-memory 5G MetaCacheSpark-0.1.0.jar -q -o -l 32 DatabaseName Output Genomica/inputs/skin9/inputSmall.fasta