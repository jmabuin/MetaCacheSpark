#!/usr/bin/env bash

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors 8 --master yarn --deploy-mode cluster --executor-memory 8G --driver-memory 5G MetaCacheSpark-0.2.0.jar -b -o -j -l 8 -t Genomica/genomes/taxonomy/ DatabaseViral Genomica/genomes/refseq/viralsmall/
