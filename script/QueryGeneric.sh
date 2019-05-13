#!/bin/bash

INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003602_S4_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003602_S4_reverse_paired.fq"
PARTITION_NUMBER=32
EXECUTOR_MEM="30G"
TMP_DATASET="S4"
THREADS_NUM=1
OUTPUT_DIR="Genomica/Output_${TMP_DATASET}_${THREADS_NUM}Thread_${PARTITION_NUMBER}Ex"
DATABASE_NAME=DatabaseNativeAFS20_$PARTITION_NUMBER
BUFFER_SIZE=500000

TMP_DATASET="S4"

if [ "$#" -eq 1 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Number of INPUT threads is: 1"
    echo "Input data set is: S4"

    PARTITION_NUMBER=$1
    TMP_DATASET="S4"

elif [ "$#" -eq 2 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Number of INPUT threads is: 1"
    echo "Input data set is: $2"

    PARTITION_NUMBER=$1
    TMP_DATASET=$2

elif [ "$#" -eq 3 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Number of INPUT threads is: $3"
    echo "Input data set is: $2"

    PARTITION_NUMBER=$1
    TMP_DATASET=$2
    THREADS_NUM=$3

elif [ "$#" -eq 4 ]; then
    echo "Number of INPUT executors is: $1"
    echo "Number of INPUT threads is: $3"
    echo "Input data set is: $2"
    echo "Database is: $4"

    PARTITION_NUMBER=$1
    TMP_DATASET=$2
    THREADS_NUM=$3
    DATABASE_NAME="DatabaseNative${4}_${PARTITION_NUMBER}"

fi


case "$TMP_DATASET" in
        S4)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003602_S4_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003602_S4_reverse_paired.fq"
            ;;

        S5)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003610_S5_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003610_S5_reverse_paired.fq"
            ;;

        S6)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003628_S6_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003628_S6_reverse_paired.fq"
            ;;

        S7)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003636_S7_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003636_S7_reverse_paired.fq"
            ;;

        S8)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003644_S8_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003644_S8_reverse_paired.fq"
            ;;

        S9)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003652_S9_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003652_S9_reverse_paired.fq"
            ;;

        S10)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003660_S10_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003660_S10_reverse_paired.fq"
            ;;
        S11)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003678_S11_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003678_S11_reverse_paired.fq"
            ;;
        S12)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003686_S12_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003686_S12_reverse_paired.fq"
            ;;
        S13)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003694_S13_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003694_S13_reverse_paired.fq"
            ;;
        S14)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003701_S14_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003701_S14_reverse_paired.fq"
            ;;
        S15)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003719_S15_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003719_S15_reverse_paired.fq"
            ;;
        S16)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003727_S16_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003727_S16_reverse_paired.fq"
            ;;

        KAL)
            INPUT_SEQUENCES="Genomica/InputDatasets/AFS/Kal_D/raw_lane2_R1.fq Genomica/InputDatasets/AFS/Kal_D/raw_lane2_R2.fq"
            ;;
        test)
            INPUT_SEQUENCES="Genomica/InputDatasets/test_Forward.fq Genomica/InputDatasets/test_Reverse.fq"
            ;;
        *)
            echo $"Usage: $0 {start|stop|restart|condrestart|status}"
            exit 1

esac


case "$PARTITION_NUMBER" in
        32)
            EXECUTOR_MEM="40G"
            ;;

        64)
            EXECUTOR_MEM="30G"
            ;;

        100)
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

OUTPUT_DIR="Genomica/Output_${DATABASE_NAME}_${TMP_DATASET}_${THREADS_NUM}Thread_${PARTITION_NUMBER}Ex"
#DATABASE_NAME=DatabaseNativeAFSBig_$PARTITION_NUMBER


if [ "$THREADS_NUM" -eq 1 ]; then
    echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory $EXECUTOR_MEM --driver-memory 30G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -a species -o -b $BUFFER_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES

else
    echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --executor-cores $THREADS_NUM --master yarn --deploy-mode cluster --executor-memory $EXECUTOR_MEM --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -n $THREADS_NUM -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
    spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --executor-cores $THREADS_NUM --master yarn --executor-memory $EXECUTOR_MEM --driver-memory 30G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -a species -o -b $BUFFER_SIZE -n $THREADS_NUM -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES

fi

