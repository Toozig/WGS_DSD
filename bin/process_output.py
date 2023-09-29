#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np
import concurrent.futures


NUM_WORKERS = 16 
NULL_STRING = ' '
NULL_INT = -1

# the index of ALT variant in vcf's AD (Allelic depth)
ALT = 1


def concat_csv_files(csv_files):
    dfs = []  # List to store individual DataFrames from each CSV file

    # Read and append each CSV file to the list
    dfs = run_parallel(lambda file: pd.read_csv(file, sep='\t', low_memory=False), csv_files)


    # Concatenate DataFrames into a single DataFrame
    concatenated_df = pd.concat(dfs, ignore_index=True)

    
    # Sort the concatenated DataFrame by CHROM, POS, REF, and ALT
    concatenated_df.sort_values(by=['CHROM', 'POS', 'REF', 'ALT'], inplace=True)

    return concatenated_df


def get_AB(df):
    # Split the 'A' column to get individual numbers and convert to integer
    a_numbers = np.asarray(df.iloc[:,0].str.split('/').apply(lambda x: list(map(int, x))).tolist())

    # Split the 'B' column to get individual numbers and convert to integer
    b_numbers = np.asarray(df.iloc[:,1].str.split(',').apply(lambda x: list(map(int, x))).tolist())

    # Calculate the ratio of legit numbers to all numbers

    # print(b_numbers.head())

    legit_numbers = a_numbers * b_numbers
    total_numbers = np.sum(b_numbers, axis=1)

    # Avoid division by zero by setting the ratio to 0 when the denominator is 0
    ratio = np.where(total_numbers == 0, 0, np.sum(legit_numbers, axis=1) / total_numbers)

    return ratio


def calc_sample_ab(s,df_in):
    print(s)
    cur_df = df_in[[f'{s}:GT',f'{s}:AD']].copy().replace(' ','0/0').replace(np.nan,'0,0')
    AB = get_AB(cur_df)
    df_in.loc[:,f'{s}:AD'] = AB

    
def check_table(input_file):
        # Read the TSV file into a pandas DataFrame
    df = pd.read_csv(input_file, sep='\t')

    
    df.columns = [col.split(']')[1] for col in df.columns]
    


def sort_df(df):
    samples = list(set([i.split(':')[0] for i in df.columns if ":" in i]))
    samples.sort()
    columns = np.asarray([[f"{s}:GT",f"{s}:DP",f"{s}:GQ",f"{s}:AB"] for s in samples]).flatten()
    return df[np.concatenate((df.columns[:df.shape[1] - len(columns)],columns))]

def run_parallel(func, iterabele, to_return=True):
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit the function for each item in the list
        # This starts the parallel execution
        futures = [executor.submit(func , item) for item in iterabele]

        # Wait for all tasks to complete and retrieve the results
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    if to_return:
        return results


def replace_sample_values(df_in, sample, replacement_dict, int_replace_dict):
        df = df_in[[f"{sample}:GT",f"{sample}:DP",f"{sample}:GQ",f"{sample}:AD"]].copy()

        ref_index = (df[f"{sample}:GT"] == '0/0') |  (df[f"{sample}:GT"] == '0|0') 
        df.loc[df.index[ref_index],[f'{sample}:DP',f'{sample}:GQ']] = NULL_INT
        
        df[[f'{sample}:DP',f'{sample}:GQ']] = df[[f'{sample}:DP',f'{sample}:GQ']].replace(int_replace_dict).astype(int) 
        df[f'{sample}:GT'] = df[f'{sample}:GT'].replace(replacement_dict).astype(str).str.replace('|','/')
        return df



def replace_missing_values(df_in):
    df = df_in.copy()
    
    
    replacement_dict = {'.': NULL_STRING, './.': NULL_STRING, '.|.': NULL_STRING, np.nan: NULL_STRING, None: NULL_STRING}
    int_replace_dict = {'.':NULL_INT, np.nan:NULL_INT,None:NULL_INT, NULL_STRING:NULL_INT}


    sample_col = set([i.split(':')[0] for i in df.columns if ':' in i])
    sample_df  = run_parallel(lambda sample: replace_sample_values(df, sample, replacement_dict, int_replace_dict), sample_col)
    sample_df = pd.concat(sample_df, axis=1).reset_index(drop=True)
    df = df.drop(columns=df.columns[df.columns.str.contains(':')]).reset_index(drop=True)

    float_col =  ['AF','AF_popmax']
    df[float_col] = df[float_col].replace(int_replace_dict).astype("float64")
    
    str_col = ['CHROM','REF','ALT','FILTER','INTERVAL_ID','GHid','GH_type']
    df[str_col] = df[str_col].replace(replacement_dict).astype(str)
    
    df['GH_is_elite'] = df['GH_is_elite'].replace(int_replace_dict).astype('int64')
    df['POS'] = df['POS'].astype('int')
    df =  pd.concat([df,sample_df],axis=1)
    return df


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
    
