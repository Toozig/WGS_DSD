#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np

NULL_STRING = ' '
NULL_INT = -1

# the index of ALT variant in vcf's AD (Allelic depth)
ALT = 1

def concat_csv_files(csv_files):
    dfs = []  # List to store individual DataFrames from each CSV file

    # Read and append each CSV file to the list
    for file in csv_files:
        df = pd.read_csv(file, sep='\t')
        dfs.append(df)

    # Concatenate DataFrames into a single DataFrame
    concatenated_df = pd.concat(dfs, ignore_index=True)

    
    # Sort the concatenated DataFrame by CHROM, POS, REF, and ALT
    concatenated_df.sort_values(by=['CHROM', 'POS', 'REF', 'ALT'], inplace=True)

    return concatenated_df





def replace_missing_values(df_in):
    df = df_in.copy()
    sample_col = set([i.split(':')[0] for i in df.columns if ':' in i])
    
    replacement_dict = {'.': NULL_STRING, './.': NULL_STRING, '.|.': NULL_STRING, np.nan: NULL_STRING, None: NULL_STRING}
    int_replace_dict = {'.':NULL_INT, np.nan:NULL_INT,None:NULL_INT, NULL_STRING:NULL_INT}
    for i in sample_col:
        ref_index = (df[f"{i}:GT"] == '0/0') |  (df[f"{i}:GT"] == '0|0') 
        df.loc[df.index[ref_index],[f'{i}:DP',f'{i}:GQ']] = NULL_INT
        df[[f'{i}:DP',f'{i}:GQ']] = df[[f'{i}:DP',f'{i}:GQ']].replace(int_replace_dict).astype(int) 
        df[f'{i}:GT'] = df[f'{i}:GT'].replace(replacement_dict).astype(str).str.replace('|','/')
        
    float_col =  ['AF','AF_popmax']
    df[float_col] = df[float_col].replace(int_replace_dict).astype("float64")
    
    str_col = ['CHROM','REF','ALT','FILTER','INTERVAL_ID','GHid','GH_type']
    df[str_col] = df[str_col].replace(replacement_dict).astype(str)
    
    df['GH_is_elite'] = df['GH_is_elite'].replace(int_replace_dict).astype('int64')
    df['POS'] = df['POS'].astype('int')

    return df




def get_AB(df):

    # Split the 'A' column to get individual numbers and convert to integer
    a_numbers = np.asarray(df.iloc[:,0].str.split('/').apply(lambda x: list(map(int, x))).tolist())

    # Split the 'B' column to get individual numbers and convert to integer
    b_numbers = np.asarray(df.iloc[:,1].str.split(',').apply(lambda x: list(map(int, x))).tolist())

    # Calculate the ratio of legit numbers to all numbers
    print(a_numbers)
    # print(b_numbers.head())
    exit
    legit_numbers = a_numbers * b_numbers
    total_numbers = np.sum(b_numbers, axis=1)

    # Avoid division by zero by setting the ratio to 0 when the denominator is 0
    ratio = np.where(total_numbers == 0, 0, np.sum(legit_numbers, axis=1) / total_numbers)

    return ratio

def calculate_AB(df_in : pd.DataFrame):
    """
    Calculates Allele Ballance  - the ratio of reads aligned at a variant locus that support the alternate allele 
    """

    samples = set([i.split(':')[0] for i in df.columns if ':' in i])
    for s in samples:
        cur_df = df_in[[f'{s}:GT',f'{s}:AD']].copy().replace(' ','0/0').replace(np.nan,'0,0')
        AB = get_AB(cur_df)
        df_in[f'{s}:AD'] = AB

    df_in.columns = df_in.columns.str.replace('AD','AB')
    return df_in


    
def check_table(input_file):
        # Read the TSV file into a pandas DataFrame
    df = pd.read_csv(input_file, sep='\t')

    
    df.columns = [col.split(']')[1] for col in df.columns]
    


def sort_df(df):
    samples = list(set([i.split(':')[0] for i in df.columns if ":" in i]))
    samples.sort()
    columns = np.asarray([[f"{s}:GT",f"{s}:DP",f"{s}:GQ",f"{s}:AB"] for s in samples]).flatten()
    return df[np.concatenate((df.columns[:df.shape[1] - len(columns)],columns))]
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Error: No input file provided.")
        print("Usage: python script_name.py input_file.tsv")
        sys.exit(1)

    input_files = sys.argv[2:]
    output_name =  sys.argv[1]
    df = concat_csv_files(input_files)
    print('done concat\nprocessing')
    df = replace_missing_values(df)
    print('calculate_AB')
    df = calculate_AB(df)
    print('sorting')
    df = sort_df(df)
    print('saving')
    df.to_parquet(f'{output_name}.parquet')
    print(f"Parquet file '{output_name}.parquet' created successfully!")
    
