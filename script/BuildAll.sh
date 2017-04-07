#!/bin/bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 32 --master yarn --deploy-mode cluster --executor-memory 8G --driver-memory 5G MetaCacheSpark-0.2.0.jar -b -u -j -l 32 -t Genomica/genomes/taxonomy/ DatabaseName Genomica/genomes/refseq/