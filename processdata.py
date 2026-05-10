import pandas as pd
import os
from functools import reduce
import pandas as pd
import ta
from sklearn.preprocessing import StandardScaler

## Joining CSVs
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


## Feature Engineering
df.sort_values("date", inplace=True, ascending=True)

df["target"] = df["lmt_close"].shift(-1) - df["lmt_open"].shift(-1)
df["target"] = df["target"].apply(lambda x: 1 if x > 0 else 0)

df = ta.add_all_ta_features(df, open="lmt_open", high="lmt_high", low="lmt_low", close="lmt_close", volume="lmt_volume")
df["oil_cci"] = ta.trend.cci(df["oil_high"], df["oil_low"], df["oil_close"], window=10)
df["oil_rsi"] = ta.momentum.rsi(df["oil_close"], window=14)
df["lmt_rsi"] = ta.momentum.rsi(df["lmt_close"], window=14)
df["lmt_MACD"] = ta.trend.macd_diff(df["lmt_close"], window_slow=26, window_fast=12, window_sign=9)
df["lmt_MACD_signal"] = ta.trend.macd_signal(df["lmt_close"], window_slow=26, window_fast=12, window_sign=9)
df["SPY_rsi"] = ta.momentum.rsi(df["spy_close"], window=14)
df["RTX_CMF"] = ta.volume.chaikin_money_flow(df["rtx_high"], df["rtx_low"], df["rtx_close"], df["rtx_volume"], window=20)
df["RTX_RSI"] = ta.momentum.rsi(df["rtx_close"], window=14)
df["QQQ_awesome_oscillator"] = ta.momentum.awesome_oscillator(df["qqq_high"], df["qqq_low"], window1=5, window2=34)
df["QQQ_rsi"] = ta.momentum.rsi(df["qqq_close"], window=14)
#df["noc_EMA"] = ta.trend.ema_indicator(df["noc_close"], window=12)
df.drop(columns=["trend_psar_up", "trend_psar_down", "trend_psar_up_indicator", "trend_psar_down_indicator"], inplace=True)
#df.dropna(inplace=True)
df = df.dropna(inplace=False)

#df.to_csv("clean_data/final_data.csv")
## Summary
print(df.head())
print(df.tail())
print(df.shape)


## Split data into training, validation, and testing
n = len(df)
train_end = int(n * 0.70)
val_end   = int(n * 0.85)
train_end_date = df["date"].iloc[train_end]
val_end_date   = df["date"].iloc[val_end]

train_df = df[df["date"] < train_end_date]
val_df = df[(df["date"] >= train_end_date) & (df["date"] < val_end_date)]
test_df = df[df["date"] >= val_end_date]

X_train = train_df.drop(columns=["date", "target"])
y_train = train_df["target"]

X_val = val_df.drop(columns=["date", "target"])
y_val = val_df["target"]

X_test = test_df.drop(columns=["date", "target"])
y_test = test_df["target"]


## Normalize data
# We have to normalize training data first, then use that 
# mean and standard deviation to normalize validation and test data

scaler = StandardScaler()
X_train_normal = scaler.fit_transform(X_train)
X_val_normal = scaler.transform(X_val)
X_test_normal = scaler.transform(X_test)


## Save files
pd.DataFrame(X_train_normal, columns=X_train.columns).to_csv("clean_data/X_train.csv", index=False)
pd.DataFrame(X_val_normal, columns=X_val.columns).to_csv("clean_data/X_val.csv", index=False)
pd.DataFrame(X_test_normal, columns=X_test.columns).to_csv("clean_data/X_test.csv", index=False)

y_train.to_csv("clean_data/y_train.csv", index=False)
y_val.to_csv("clean_data/y_val.csv", index=False)
y_test.to_csv("clean_data/y_test.csv", index=False)
