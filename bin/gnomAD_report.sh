#!/bin/bash

################################################################################
# Script Name: chromosome_variant_checker.sh
# Description: This script checks if the latest variant of each chromosome from
#              a local file is similar to the corresponding variant in the
#              gnomAD database.
#
# Usage: bash chromosome_variant_checker.sh <region_file> <region_simple_name> <local_gnomAD_folder> <output>
#
# Arguments:
#   region_file         Path to the region file containing chromosome information.
#   region_simple_name  Simple name of the region.
#   local_gnomAD_folder Path to the folder where local gnomAD data stored
#   output              Name of the output file to store results.
#
# Output:
#   The script outputs the name of the output file with comparison results.

################################################################################

region_file=$1
region_simple_name=$2
local_gnomAD_folder=$3
output=$4



standard_chromosomes=("chr1" "chr2" "chr3" "chr4" "chr5" "chr6" "chr7" "chr8" "chr9" "chr10" "chr11" "chr12" "chr13" "chr14" "chr15" "chr16" "chr17" "chr18" "chr19" "chr20" "chr21" "chr22" "chrX" "chrY")
for  chrom in "${standard_chromosomes[@]}"; do

    local_gnomAD="$local_gnomAD_folder/$chrom.$region_simple_name.tsv.gz"
    # Print the current chromosome being processed
    echo "$chrom"
    
    # Append the current chromosome to the $output file
    echo "$chrom" >> $output
    
    # Find the last peak (line) in the region file file for the current chromosome
    last_peak=$(grep -P "$chrom\t" $region_file | tail -n 1)
    # Append the last peak information to the $output file
    echo "$last_peak" >> $output

    # Find the last variant (line) in the corresponding gnomAD data file for the current chromosome
    last_variant=$(zcat  $local_gnomAD | tail -n 1)
    # Append a label and the last variant information to the $output file
    echo "last variant in our file" >> $output
    echo "$last_variant" >> $output

    # Convert last_peak into a tab-separated format and store in tmp.bed
    echo -e $last_peak | tr ' ' '\t' >> tmp.bed
    # Define the URL of the gnomAD VCF file
    VCF="https://gnomad-public-us-east-1.s3.amazonaws.com/release/3.1.2/vcf/genomes/gnomad.genomes.v3.1.2.sites.${chrom}.vcf.bgz"
    # Define the desired output format for bcftools query
    format="%CHROM\\t%POS\\t%REF\\t%ALT\\t%FILTER\\t%AF\\t%AF_popmax\\n"
    # Append a label and the last 2 variant information from the gnomAD VCF to the $output file
    echo "last 2 variant in gnomAD vcf" >> $output
    bcftools query -f "${format}" -R tmp.bed ${VCF} | tail -n 1 >> $output
    # Remove tmp.bed
    rm -rf tmp.bed
    
    # Append a separator for better readability in the $output file
    echo "----------------------------------" >> $output
done
