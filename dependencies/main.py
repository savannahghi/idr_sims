from google.cloud import storage
import pandas as pd
from submissions import *
from forms import formIds

formIds = formIds

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    ''' 
    The ID of your GCS bucket
    bucket_name = "your-bucket-name"

    The contents to upload to the file
    contents = "these are my contents"

    The ID of your GCS object
    destination_blob_name = "storage-object-name"
    '''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )

def submissions_to_storage():
    dfs = []
    for form_id in formIds:
        submissions = fullSubmissions(form_id)
        dfs.append(submissions)

    s = -1
    data = []
    for df in dfs:
        df.columns= df.columns.str.replace('.','_',regex=True)
        a = df.loc[:,df.columns.str.endswith('SCORE')]
        def scores(score):
            if score == "red":
                return 1
            elif score == "yellow":
                return 2
            elif score == "green":
                return 3
        for i in list(a.columns):
            a[i] = a[i].apply(scores)
        a = a.add_suffix('_grade')
        a["Set_Average"] = a.mean(axis=1)
        a = a.assign(Expected_Average=3.0)
        a["Average_Score"] = ((a["Set_Average"] / a["Expected_Average"]))
        a["Set_ID"] = formIds[s+1]
        df = pd.merge(df, a, left_index=True, right_index=True)
        data.append(df)
        item = formIds[s+1]
        upload_blob_from_memory(
        bucket_name="odk_test",
        contents=df.to_parquet(),
        destination_blob_name=f"SIMS_parquet/{item}.parquet",)
        s = s + 1

    appended_data = pd.concat(data, ignore_index=True)
    appended_data = appended_data[[col for col in appended_data.columns if any(
        s in col for s in ['ASSESSMENT', 'COMM', 'SCORE', 'grade', 'Set_Average', 'Expected_Average', 'Average_Score','Facility'])]]
    upload_blob_from_memory(
        bucket_name="odk_test",
        contents=appended_data.to_parquet(),
        destination_blob_name=f"SIMS_parquet/big_table.parquet",)