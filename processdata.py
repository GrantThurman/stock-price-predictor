import pandas as pd
import os
from functools import reduce


def load(path, prefix):
    df = pd.read_csv(path)


    df.columns = [c.lower().strip().replace('"','') for c in df.columns]

    if 'datetime' in df.columns:
        df = df.rename(columns={'datetime': 'date'})

    df = df.rename(columns={
        'price': 'close',
        'close price': 'close',
        'closing price': 'close',
        'vol.': 'volume',
        'vol': 'volume',
        'change %': 'change_percent'
    })

    df['date'] = pd.to_datetime(df['date'])
    for col in df.columns:
        if col == 'date':
            continue
        df[col] = df[col].astype(str)
        df[col] = df[col].str.replace(',', '')
        df[col] = df[col].str.replace('K', 'e3')
        df[col] = df[col].str.replace('M', 'e6')
        df[col] = df[col].str.replace('B', 'e9')
        df[col] = df[col].str.replace('%', '')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.sort_values('date')
    rename_map = {col: f"{prefix}_{col}" for col in df.columns if col != 'date'}
    df = df.rename(columns=rename_map)

    return df

data_dir = "data"
dfs = []
print("files beign loaded")
for file in sorted(os.listdir(data_dir)):
    print(file)

for file in os.listdir(data_dir):
    if file.endswith(".csv"):
        path = os.path.join(data_dir, file)
        
        prefix = file.replace(".csv", "").lower()
        
        df = load(path, prefix)
        dfs.append(df)
df = reduce(lambda left, right: pd.merge(left, right, on='date', how='inner'), dfs)
df = df.sort_values('date')
df = df.ffill()
df = df.dropna()
df = df.rename(columns={
    'close price': 'close',
    'closing price': 'close',
    'vol': 'volume',
    '% change': 'change_percent'
})

print("Loaded assets:")
for file in os.listdir(data_dir):
    if file.endswith(".csv"):
        print("-", file)
feature_count = len(df.columns) - 1
print("Total features:", feature_count)


print(df.isna().sum())
print(df.head())
print(df.tail())
print(df.columns)
print(df.shape)
print(df.columns.tolist())
