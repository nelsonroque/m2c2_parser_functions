# IMPORTS ---------------------------------------
import os
import re
import sys
import time
from datetime import datetime
import hashlib
from functools import reduce
import pandas as pd
from pandas.core.common import flatten
import json
import urllib.parse
from glob import glob
from io import StringIO # python3; python2: BytesIO

import os.path
from os import path

# UTILITY FUNCTIONS --------------------------------------
def generate_system_time(to_str=True):
  if to_str:
      return datetime.now().isoformat()
  else:
      return datetime.now()

# PARSING FUNCTIONS -----------------------------
def get_m2c2_file_type(filename=None):
  # determine type of input file
  filename_c = os.path.basename(filename)
  input_file_metadata = filename_c.split("_")
  # specify types of data files based on filename pattern
  if input_file_metadata[0] == "cogtask" or input_file_metadata[0] == "data/cogtask":
    input_file_parse_type = "COGNITIVE_DATA"
  elif input_file_metadata[0] == "data" or input_file_metadata[0] == "data/data":
    input_file_parse_type = "SURVEY_DATA"
  else:
    input_file_parse_type = "OTHER_DATA"
  return(input_file_parse_type)

# DATA PARSING FUNCTIONS --------------------------------------
def split_to_list(data=None, delim="\r\n", log=True, session_uuid=None):
    # split the data at each carriage return and new line
    decoded_data = data
    all_lines = decoded_data.split(delim)
    if len(all_lines) <= 1:
        delim = "\n"
        all_lines = decoded_data.split(delim)
    return all_lines

def parse_survey_data(data):
    # split body into list
    decrypted_data_lines = data
    
    # init containers
    keys = []
    vals = []
    
    # start parsing based on first colon
    for line in decrypted_data_lines:
        ls = line.split(":", 1)
        if(len(ls) == 2):
            key = ls[0]
            value = ls[1]
        elif(ls == ['']):
            #exit("Empty line")
            pass
        else:
            exit(generate_system_time() + " | Parse failure" + str(ls))
            
        # save key values
        keys.append(key)
        vals.append(value)
    return(keys, vals)
    
def parse_cognitive_data(data):
    lines = data
    PATTERN_MATCH_UNNESTED_COMMAS = ',\s*(?![^{}]*\})'
    all_rows = []
    for line in lines:
        keys = []
        vals = []
        data_list = re.split(PATTERN_MATCH_UNNESTED_COMMAS, line) # split into columns
        row_dict = {}
        for item in data_list: # for each column
            item_s = item.split(":", 1) # split each column by key value pairs
            #print(item_s)
            if(len(item_s) == 2): # if of appropriate length
                key = item_s[0]
                val = item_s[1]
            elif(item_s == ['']):
                pass
            else:
                print(item_s)
                exit("Parse failure")
            keys.append(key)
            vals.append(val)
            col_dict = dict(zip(keys,vals))
            row_dict.update(col_dict)
        all_rows.append(row_dict)
    return(all_rows)

def of_equal_length(a,b):
    if(len(a) == len(b)):
        valid = True
    else:
        valid = False
    return valid

def parser(base_path, pack_id, verbose=False):
  pfile_list = glob(base_path + '*/*/*{}*.txt'.format(pack_id), recursive = True)
  n_files = len(pfile_list)

  # start timer
  tic = time.perf_counter()
  for filen in pfile_list:
    # LOG ---
    if verbose:
      print(generate_system_time() + " | File: " + json.dumps(filen, indent=2))
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    # Check filename extension
    if filen.endswith('.txt'):
        parse_fn_csv = filen[:-4] + ".csv"
        if verbose:
          print(generate_system_time() + " | Output file: " + parse_fn_csv)
    else:
      if verbose:
        print(generate_system_time() + " | File skipped")

    # *************************************
    # logic to skip if csv exists
    # *************************************
    if path.exists(parse_fn_csv):
      continue
    else:
      # Get data type (as per M2C2 specifications for v1.3+ apps)
      data_type = get_m2c2_file_type(filen)
      if verbose:
        print(generate_system_time() + " | Data Type: " + json.dumps(data_type, indent=2))

      # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

      # *************************************
      # read file body
      # *************************************

      if verbose:
        print(generate_system_time() + " | FILE OPENING --------")
      with open(filen) as f:
          file_list = [line.rstrip('\n') for line in f]
      #print(file_list)
      if verbose:
        print(generate_system_time() + " | FILE CLOSED --------")

      # *************************************
      # parse data based on type
      # *************************************
      if data_type == "COGNITIVE_DATA":
        all_rows = parse_cognitive_data(file_list)
        pd_df = pd.DataFrame(all_rows)
      elif data_type == "SURVEY_DATA":
        keys, vals = parse_survey_data(file_list)
        # if length of keys and values matches, save data
        if of_equal_length(keys, vals):
            # Create the pandas DataFrame
            pd_df = pd.DataFrame([vals])
            #pd_df.dropna(how='all', axis=1, inplace=True) 
            pd_df.columns = keys
      pd_df.to_csv(parse_fn_csv, index=False)

  # stop timer
  toc = time.perf_counter()
  print(f"Created csv files in {toc - tic:0.4f} seconds")
  return(n_files)

def create_merged_file(base_path, pack_id):
  # start timer
  tic = time.perf_counter()

  current_csvfiles = glob(base_path + '*/*/*{}*.csv'.format(pack_id), recursive = True)
  n_files = len(current_csvfiles)

  # merge like files
  merge_df = pd.concat([pd.read_csv(f, index_col=[0,1])for f in current_csvfiles])
  merge_fn = pack_id.replace("-", "_") + generate_system_time() + ".csv"
  merge_df.to_csv(merge_fn, index=False)

  # stop timer
  toc = time.perf_counter()
  print(f"Created merged file in {toc - tic:0.4f} seconds")
  return(n_files)