#!/bin/bash

PROCESSED_FOLDER=$1
ALL_SAMPLES_FILE="${PROCESSED_FOLDER}/all_samples.txt"

rm -f $ALL_SAMPLES_FILE

find `pwd` -type f -name '*vcf.gz' > $ALL_SAMPLES_FILE