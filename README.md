## get_Sample_TSV Instructions

This pipeline is designed to process VCF files by combining them with gnomAD AF / popmax_AF information. Additionally, it incorporates data from the geneHancer DB, which can be accessed [here](https://www.weizmann.ac.il/molgen/genehancer-genome-wide-integration-enhancers-and-target-genes-genecards).

To use this script, ensure that the following directory structure is in place:

```
/
├── get_sample_TSV.nf
├── get_sample_TSV.json
├── process-selector.config (optional)
├── headers/
│   ├── interval_ID.header
│   ├── geneHancer_AnnotSV_elements.header
│   ├── gnomAD.header
├── bin/
│   ├── mergeGnomAD.sh
│   ├── getGnomAD.sh
│   ├── merge_chrom.py
│   ├── getSamples.sh
│   └── clean_tsv.py
├── data-read_only-layers_data-GeneHancer_AnnotSV_elements_v5.15.txt
```

Before executing the script, make sure to fill in the parameter file `get_sample_TSV.json` with the following values:

- `gnomADByRegionDir`: Directory where the gnomAD data should be saved or looked up if it already exists.
- `DBXCLI`: Path to [dbxcli](https://github.com/dropbox/dbxcli) for Dropbox uploads.
- `uploadDir`: The Dropbox directory for uploading.
- `VENV`: Path to the virtual environment containing Pandas and NumPy.
- `MAX_REGIONS`: The maximum number of regions per gnomAD call (e.g., 10000).
- `REGION_SPLIT_SIZE`: If regions exceed `MAX_REGIONS`, files will be split, each containing this number of regions (e.g., 6000).
- `cur_regions`: The default peak bed file.
- `all_samples`: The default file containing paths to sample CSVs (processed by the `process_VCF` pipeline), can also be defined in the command line.
- `output_dir`: The output directory of the process, can be defined in the command line.

**Inputs:**
- `sample_file`: File where each line contains the path to a processed VCF file.
- `params-file`: JSON file with parameter configurations.
- `upload`: Set to 'true' to upload to Dropbox.
- `bed_file`: Region file in BED format.

**Outputs:**
- `gnomADByRegionDir/regionFile`: Directory containing gnomAD files by region.
- `output_dir/$regionFile.simpleName/$sampleFile.simpleName/samples_raw`: Raw TSV file for samples.
- `$params.output_dir/$regionFile.simpleName/$sampleFile.simpleName/regionFile.simpleName.sampleFile.simpleName.prq`: Table with combined data.

To run the script, follow these steps:

1. Open a terminal window.

2. Navigate to the directory containing the `get_sample_TSV.nf` script.

3. Execute the following command:

```bash
~/nextflow run get_sample_TSV.nf --sample_file data/read_only/samples/processed/all_samples.txt --params-file pipeline_param.json --upload true -c process-selector.config
```

Explanation of the command:

- `~/nextflow run get_sample_TSV.nf`: Initiates the Nextflow process using the `get_sample_TSV.nf` script.
- `--sample_file data/read_only/samples/processed/all_samples.txt`: Specifies the input file containing paths to processed VCF files.
- `--params-file pipeline_param.json`: Specifies the parameter configuration JSON file.
- `--upload true`: Enables the option to upload results to Dropbox.
- `-c process-selector.config`: Optionally uses the `process-selector.config` file for configuration (if available).

Make sure you have the required files and directories in place before running the script.