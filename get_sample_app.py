import streamlit as st
import subprocess
import json
import os

# Define help text as global variables
GNOMADBYREGIONDIR_HELP = "Directory where the gnomAD data should be saved or looked up if it already exists."
DBXCLI_HELP = "Path to dbxcli for Dropbox uploads."
UPLOADDIR_HELP = "The Dropbox directory for uploading."
VENV_HELP = "Path to the virtual environment containing Pandas and NumPy."
MAX_REGIONS_HELP = "The maximum number of regions per gnomAD call."
REGION_SPLIT_SIZE_HELP = "If regions exceed MAX_REGIONS, files will be split, each containing this number of regions."
ALL_SAMPLES_HELP = "The default file containing paths to sample CSVs (processed by the process_VCF pipeline), can also be defined in the command line."
OUTPUT_DIR_HELP = "The output directory of the process, can be defined in the command line."
SAMPLE_FILE_HELP = "File where each line contains the path to a processed VCF file."
PARAMS_FILE_HELP = "JSON file with parameter configurations."
UPLOAD_HELP = "Set to 'true' to upload to Dropbox."
BED_FILE_HELP = "Region file in BED format."


def construct_command(gnomADByRegionDir, DBXCLI, uploadDir, VENV, MAX_REGIONS, REGION_SPLIT_SIZE, all_samples, output_dir, sample_file, params_file, upload, bed_file, proccess_selector=False):
    # Define the command
    command = "nextflow run get_sample_TSV.nf"

    # Add each argument to the command
    args = {
        "gnomADByRegionDir": gnomADByRegionDir,
        "DBXCLI": DBXCLI,
        "uploadDir": uploadDir,
        "VENV": VENV,
        "MAX_REGIONS": MAX_REGIONS,
        "REGION_SPLIT_SIZE": REGION_SPLIT_SIZE,
        "all_samples": all_samples,
        "output_dir": output_dir,
        "sample_file": sample_file,
        "upload": upload,
        "bed_file": bed_file
    }

    for arg, value in args.items():
        if bool(value):
            command += f" --{arg} {value}"

    # Add the config file to the command
    command += f"  -params-file {params_file}"
    if proccess_selector:
        command += "-c process-selector.config"

    return command

def check_dbxcli(DBXCLI):
    try:
        process = subprocess.Popen({DBXCLI}, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if "dbxcli: command not found" in stderr.decode():
            st.warning("dbxcli not found, please install it.")
    except:
        st.warning("dbxcli not found, please install it.")

def check_venv(VENV):
    try:
        process = subprocess.Popen(f"source {VENV}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if "No such file or directory" in stderr.decode():
            st.warning("Virtual environment not found, please install it.")
    except:
        st.warning("Virtual environment not found, please install it.")

def show_sample(sample_file):
  
    with open(sample_file, 'r') as f:
        sample_list = f.readlines()
    st.code('\n'.join(sample_list[:min(3,len(sample_list))]))


def get_input():
    # Use global variables for help text
    cur_date = datetime.datetime.now().strftime("%d_%m_%y_%H")
    gnomADByRegionDir = st.text_input('gnomADByRegionDir', value="data/pipeline_outputs/variants_gnomAD", help=GNOMADBYREGIONDIR_HELP)
    DBXCLI = st.text_input('DBXCLI', value="dbxcli", help=DBXCLI_HELP)
    uploadDir = st.text_input('uploadDir', value="Nitzan_Gonen_lab/Joint_projects/WGS_on_DSD/data/pipeline_outputs/variants_gnomAD", help=UPLOADDIR_HELP)
    VENV = st.text_input('VENV', value="/home/dsi/toozig/.virtualenvs/deepBindEnv/bin/activate", help=VENV_HELP)
    MAX_REGIONS = st.number_input('MAX_REGIONS', value=10000, help=MAX_REGIONS_HELP)
    REGION_SPLIT_SIZE = st.number_input('REGION_SPLIT_SIZE', value=6000, help=REGION_SPLIT_SIZE_HELP)
    all_samples = st.text_input('all_samples', value="data/read_only/samples/processed/all_samples.txt", help=ALL_SAMPLES_HELP)
    output_dir = st.text_input('output_dir', value=f"data/pipeline_outputs/variants_gnomAD/{cur_date}", help=OUTPUT_DIR_HELP)
    sample_file = st.text_input('sample_file', help=SAMPLE_FILE_HELP)
    params_file = st.text_input('params-file', value='get_sample_TSV.json' ,help=PARAMS_FILE_HELP)
    upload = st.checkbox('upload',value=True, help=UPLOAD_HELP)
    bed_file = st.text_input('bed_file',value= "data/read_only/ATAC_seq/merged/merged_mATAC_hATAC_0507.bed", help=BED_FILE_HELP)
    # try to run the command "dbxcli" to check if it is installed
    check_dbxcli(DBXCLI)
    check_venv(VENV)
    show_sample(all_samples)
    with open(params_file, 'r') as f:
        params = json.load(f)
    
    st.json(params)
    if len(sample_file):
        show_sample(sample_file)
    show_sample(bed_file)
    return gnomADByRegionDir, DBXCLI, uploadDir, VENV, MAX_REGIONS, REGION_SPLIT_SIZE, all_samples, output_dir, sample_file, params_file, upload, bed_file

from bin.zip_script import zip_files_in_dir
import datetime
import os


def list_files(dir):
    r = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            r.append(os.path.join(root, file))
    return r


def upload_to_dropbox(uploadDir, DBXCLI, cur_output_dir):
    # get current date in format '%d_%m_%y_%H'
    now = datetime.datetime.now()
    now = now.strftime("%d_%m_%y_%H")
    uploadDir = f"{uploadDir}/{now}"
    # Upload the files inside the output directory
    for output in list_files(cur_output_dir):
        output_base = os.path.basename(output)
        command = f"{DBXCLI} put {output} {uploadDir}/{output_base}"
        st.write(f"uploading the file - {output}")
        st.code(command, language='shell')
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        st.code(stdout.decode(), language='shell')
        st.code(stderr.decode(), language='shell')
    st.write(f"Files uploaded to Dropbox: {uploadDir}")

def main():
    # Get the input
    gnomADByRegionDir, DBXCLI, uploadDir, VENV, MAX_REGIONS, REGION_SPLIT_SIZE, all_samples, output_dir, sample_file, params_file, upload, bed_file = get_input()   

    # Create two columns for the buttons
    col1, col2 = st.columns(2)

    # Create a button for running the script in the first column
    if col1.button('Run Script'):
        # Construct the command
        command = construct_command(gnomADByRegionDir, DBXCLI, uploadDir, VENV, MAX_REGIONS, REGION_SPLIT_SIZE, all_samples, output_dir, sample_file, params_file, upload, bed_file)
        st.write(f"Running command:")
        st.code(command, language='shell')
        # Run the command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Display the output
        st.code(stdout.decode(), language='shell')
        st.code(stderr.decode(), language='shell')
        # data/pipeline_outputs/variants_gnomAD/merged_mATAC_hATAC_0507/test_samples/merged_mATAC_hATAC_0507.test_samples.parquet

        bed_file_name = os.path.basename(bed_file)
        sample_file_name = os.path.basename(sample_file) if bool(sample_file) else os.path.basename(all_samples)
        file_name = f'{bed_file_name}.{sample_file_name}.parquet'
        output_file = os.path.join(output_dir,file_name)
        # get the dir path of this script file
        script_dir = os.path.dirname(os.path.realpath(__file__))

        # Zip the output directory
        zip_path = zip_files_in_dir(script_dir, output_dir, command)
        if upload:
            upload_to_dropbox(uploadDir, DBXCLI, output_dir)
        st.write(f"Output directory zipped and uploaded to Dropbox: {zip_path}")
    
        return output_file

    # Create a button for showing the command in the second column
    if col2.button('Show Command'):
        # Construct the command
        command = construct_command(gnomADByRegionDir, DBXCLI, uploadDir, VENV, MAX_REGIONS, REGION_SPLIT_SIZE, all_samples, output_dir, sample_file, params_file, upload, bed_file)
        st.write(f"Running command:")
        st.code(command, language='shell')

if __name__ == '__main__':
    main()