import yaml
import config
import argparse
import os
import pandas as pd

def remove_unwanted_columns(df, config):
    unwanted_columns = config['unwanted_columns']['columns']
    columns_to_remove = list(filter(lambda x : x in df.columns , unwanted_columns))
    if len(columns_to_remove) > 0 :
        return df.drop(columns_to_remove)
    return df

def read_params(config_path:str)->dict :
    with open(config_path) as yaml_file :
        config = yaml.safe_load(yaml_file)
        return config


# if __name__ == "__main__" :
#     args= argparse.ArgumentParser()
#     args.add_argument("--config",default= os.path.join("config","params.yaml"))
#     parsed_args = args.parse_args()
#     remove_unwanted_columns(config=parsed_args.config)
#     read_params(config_path=parsed_args.config)