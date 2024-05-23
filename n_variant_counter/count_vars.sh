#!/bin/bash

# Check if a file name is provided
if [ $# -eq 0 ]
then
    # echo "No arguments supplied. Please provide a VCF file."
    exit 1
fi

# Get the file name
vcf_file=$1

# Check if the file exists
if [ ! -f $vcf_file ]
then
    # echo "File not found!"
    exit 1
fi

# Extract the header line with the sample names
header_line=`zcat  $vcf_file | grep "^#CHROM"`

# Get an array of the sample names
IFS=$'\t' read -r -a sample_names <<< "$header_line"

# Loop over the sample names, skipping the first 9 columns (fixed columns in VCF)
for index in "${!sample_names[@]}"

do
    if [ $index -gt 8 ]
    then
        # Extract the sample name
        sample_name=${sample_names[$index]}
        # Use bcftools to count the number of variants for this sample
        num_variants=$(bcftools view -s $sample_name $vcf_file | cut -f 10 | cut -d ':' -f 1 | grep -v "#" | grep "1" | wc -l)
        # Output the result
        echo -e "${sample_name}\t${num_variants}"
    fi
done
