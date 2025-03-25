import pandas as pd
import json

def model(dbt, session):
    raw = dbt.ref("health_hospitals").to_pandas()

    data = raw['BEDS'].astype(int).sum()

    return pd.DataFrame([[data]], columns=['BEDS'])
