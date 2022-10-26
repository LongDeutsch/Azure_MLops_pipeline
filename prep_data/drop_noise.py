import argparse
import os
from azureml.core import Run

print("Drop duplicate data")

run = Run.get_context()
raw_data = run.input_datasets["raw_data"]

parser = argparse.ArgumentParser("drop_noise")
parser.add_argument("--output_dropNoise", type=str, help="Remove duplicate data")

args = parser.parse_args()
print("Argument (output drop noise data path): %s" % args.output_dropNoise)

columns = ['Income','Age','Experience','Married/Single','House_Ownership',
            'Car_Ownership','Profession','CITY','STATE','CURRENT_JOB_YRS',
            'CURRENT_HOUSE_YRS','Risk_Flag']
loan_df = (raw_data.to_pandas_dataframe().dropna(how='all'))[columns]

# remove duplicate data
attributes=['Income','Age','Experience','Married/Single','House_Ownership',
            'Car_Ownership','Profession','CITY','STATE','CURRENT_JOB_YRS',
            'CURRENT_HOUSE_YRS' ]
print('Trước khi loại bỏ nhiễu:',loan_df.shape)
loan_df = loan_df.drop_duplicates(subset=attributes)
print('Sau khi loại bỏ nhiễu:',loan_df.shape)          

if not (args.output_dropNoise is None):
    os.makedirs(args.output_dropNoise, exist_ok=True)
    print("%s created" % args.output_dropNoise)
    path = args.output_dropNoise + "/processed.parquet"
    write_df = loan_df.to_parquet(path)
