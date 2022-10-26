import argparse
import os
from azureml.core import Run
from sklearn import preprocessing
import pandas as pd

print("Robust scalling data")

run = Run.get_context()


encoded_data = run.input_datasets["encoded_data"]
loan_df = encoded_data.to_pandas_dataframe()
loan_df.reset_index(drop=True, inplace=True)
df = loan_df

parser = argparse.ArgumentParser("robust_scalling")
parser.add_argument("--output_scale", type=str, help="Robust scalling data")

args = parser.parse_args()
print("Argument (output scalling data path): %s" % args.output_scale)

# Appending yellow data to green data
attributes=['Income','Age','Experience','Married/Single','House_Ownership',
            'Car_Ownership','Profession','CITY','STATE','CURRENT_JOB_YRS',
            'CURRENT_HOUSE_YRS']
            
scaler = preprocessing.RobustScaler()
loan_df = scaler.fit_transform(loan_df[attributes])
loan_df = pd.DataFrame(loan_df, columns=attributes)
loan_df = pd.concat([loan_df, df['Risk_Flag']], axis=1)

if not (args.output_scale is None):
    os.makedirs(args.output_scale, exist_ok=True)
    print("%s created" % args.output_scale)
    path = args.output_scale + "/processed.parquet"
    write_df = loan_df.to_parquet(path)
