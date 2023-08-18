#!/bin/bash
# This script performs various checks related to a specific genetic position using gnomAD data.
# If ref and alt are not provided, all variants at the specified position will be printed.


# Define paths and filenames

region_file=$(cat "../../current_work_files.json" | jq -r ".peak_file")
region_simple_name=`basename "$region_file" .bed`

gnomAD_folder="data/read_only/variant_gnomAD/$region_simple_name"



# Check if the required command line arguments are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <chromosome_num> <position> [ref_base] [alt_base]"
    exit 1
fi

# Assign input arguments to variables
nchom=$1
chrom="chr$1"
pos=$2
ref=$3
alt=$4

# Display information about the position
echo -e "\n\tLooking for position $chrom:$pos ref - $ref alt - $alt\n"

# Display gnomAD variant information link
if [ -n "$ref" ] && [ -n "$alt" ]; then
    echo -e "gnomAD -\n\t https://gnomad.broadinstitute.org/variant/$nchom-$pos-$ref-$alt?dataset=gnomad_r3\n"
else
    echo -e "gnomAD -\n\t https://gnomad.broadinstitute.org/region/$nchom-$pos-$pos?dataset=gnomad_r3\n"
fi


# Load necessary modules
module load hurcs bedtools bcftools

# Create a temporary bed file with the queried position
echo -e "$chrom\t$(($pos - 1))\t$pos" > query_position.bed

# Find intersection between the region file and the query position
intersection=$(bedtools intersect -wa -a $region_file -b query_position.bed)

if [ -n "$intersection" ]; then
    echo -e "\tThe position is found in:"
    echo -e "$intersection\n"
else
    echo -e "\tPosition is not in the bed file $region_file"
fi

# Extract the name of the region from the path
region_name=$(basename $gnomAD_folder)
gnomAD_file="${gnomAD_folder}/${chrom}.${region_name}.tsv.gz"

# Check if the position exists in the local gnomAD file
tab_pos="$chrom\t$pos"
if [ -n "$ref" ] && [ -n "$alt" ]; then
    tab_pos="${tab_pos}\t$ref\t$alt"
fi

echo -e "\n\tLooking in local gnomAD processed file"
output=$(zcat "$gnomAD_file" | grep -P "$tab_pos")

if [ -n "$output" ]; then
    echo -e "\tFound in local gnomAD"
    echo "$output"
else
    echo -e "\tNot found in local gnomAD"
fi

# Define URL of the gnomAD VCF file
VCF="https://gnomad-public-us-east-1.s3.amazonaws.com/release/3.1.2/vcf/genomes/gnomad.genomes.v3.1.2.sites.${chrom}.vcf.bgz"
format="%CHROM\\t%POS\\t%REF\\t%ALT\\t%FILTER\\t%AF\\t%AF_popmax\\n"

# Query gnomAD VCF for the specific position
gnomAD_db=$(bcftools query -f "${format}" -R query_position.bed ${VCF})

if [ -n "$gnomAD_db" ]; then
    echo -e "\tgnomAD site data:"
    echo -e "$gnomAD_db" | grep -P "$tab_pos"
else
    echo -e "\tPosition not found in gnomAD VCF file"
fi

# Cleanup: remove temporary files
rm -f query_position.bed $VCF.tbi
