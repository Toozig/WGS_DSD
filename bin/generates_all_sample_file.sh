#!/bin/bash

cd $1

PROCESSED_FOLDER=`pwd`

ALL_SAMPLES_FILE="${PROCESSED_FOLDER}/all_samples.txt"

rm -f $ALL_SAMPLES_FILE

find $PROCESSED_FOLDER -type f -name '*vcf.gz'  > $ALL_SAMPLES_FILE