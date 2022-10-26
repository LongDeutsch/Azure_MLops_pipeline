import argparse
import os
from azureml.core import Run
from sklearn import preprocessing
le = preprocessing.LabelEncoder()

print("Encode label from string to int datatype of the input data")

# Get the input green_taxi_data. To learn more about how to access dataset in your script, please
# see https://docs.microsoft.com/en-us/azure/machine-learning/how-to-train-with-datasets.
run = Run.get_context()
new_df = run.input_datasets["clean_data"]
new_df = new_df.to_pandas_dataframe()

parser = argparse.ArgumentParser("encode")
parser.add_argument("--output_encode", type=str, help="encode loan data directory")

args = parser.parse_args()
          
cols = ['Married/Single','House_Ownership','Car_Ownership','Profession','CITY','STATE']
for i in cols:
    new_df[i] = le.fit_transform(new_df[i]) 

new_df.reset_index(inplace=True, drop=True)
path = args.output_encode + "/processed.parquet"
print(path)
if not (args.output_encode is None):
    os.makedirs(args.output_encode, exist_ok=True)
    print("%s created" % args.output_encode)
    path = args.output_encode + "/processed.parquet"
    write_df = new_df.to_parquet(path)
    print(path)
