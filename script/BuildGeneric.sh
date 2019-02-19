#!/bin/bash

INPUT_SEQUENCES="Genomica/InputDatasets/AFS/RefSeq"
PARTITION_NUMBER=32
EXECUTOR_MEM="35G"
TMP_DATASET="NORMAL" #NORMAL, BIG, BIGGER, REFSEQ
OUTPUT_DIR="Database_${TMP_DATASET}_${PARTITION_NUMBER}Ex"
TAXONOMY="Genomica/InputDatasets/RefSeq90/Refseq-Release90-Taxonomy"


if [ "$#" -eq 1 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Input data set is: NORMAL"

    PARTITION_NUMBER=$1
    TMP_DATASET="NORMAL"

elif [ "$#" -eq 2 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Input data set is: $2"

    PARTITION_NUMBER=$1
    TMP_DATASET=$2

fi


case "$TMP_DATASET" in
        NORMAL)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/RefSeq"
            ;;

        BIG)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/RefSeqBig"
            ;;

        BIGGER)
            INPUT_SEQUENCES="Genomica/InputDatasets/BigDataset"
            ;;

        REFSEQ)
            INPUT_SEQUENCES="Genomica/InputDatasets/RefSeq90/Refseq-Release90/refseq"
            ;;

        *)
            echo $"Usage: $0 {start|stop|restart|condrestart|status}"
            exit 1

esac


case "$PARTITION_NUMBER" in
        32)
            EXECUTOR_MEM="30G"
            ;;

        64)
            EXECUTOR_MEM="25G"
            ;;

        128)
            EXECUTOR_MEM="20G"
            ;;

        200)
            EXECUTOR_MEM="15G"
            ;;

        *)
            echo "Number of executors: $PARTITION_NUMBER is not correct"
            exit 1

esac

OUTPUT_DIR="Database_${TMP_DATASET}_${PARTITION_NUMBER}Ex"


if [ "$THREADS_NUM" -eq 1 ]; then
    echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES

else
    echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --executor-cores $THREADS_NUM --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -n $THREADS_NUM -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --executor-cores $THREADS_NUM --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -n $THREADS_NUM -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES

fi

