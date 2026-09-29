"""Test whether more historical data improves next-day LMT direction accuracy.

Basically, The target is whether the 'following' trading day's close exceeds its open.
Features are known after the current trading day closes. The original
training/validation/test calendar boundaries are kept for comparison.

Run using terminal command line: python direction_model_experiment.py --csv final_data.csv
"""

import argparse

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


TICKERS = ["LMT", "NOC", "RTX", "SPY", "QQQ"]
VAL_START = pd.Timestamp("2024-09-26")
TEST_START = pd.Timestamp("2025-08-07")
TEST_END = pd.Timestamp("2026-04-10")


def download_history(start):
    try:
        import yfinance as yf
    except ImportError as error:
        raise SystemExit("Install yfinance first: python -m pip install yfinance") from error

    prices = yf.download(
        TICKERS, start=start, end="2026-04-14", auto_adjust=True,
        progress=False, threads=False,
    )
    if prices.empty:
        raise ValueError("No market data was downloaded")

    # yfinance returns column levels (price field, ticker) for multiple stocks.
    required = {(field, ticker) for field in ["Open", "Close"] for ticker in TICKERS}
    if not isinstance(prices.columns, pd.MultiIndex) or not required.issubset(set(prices.columns)):
        raise ValueError("Download is missing required open/close columns")
    frame = pd.DataFrame({"date": pd.to_datetime(prices.index)})
    for ticker in TICKERS:
        frame[f"{ticker.lower()}_open"] = prices[("Open", ticker)].to_numpy()
        frame[f"{ticker.lower()}_close"] = prices[("Close", ticker)].to_numpy()
    return frame


def make_features(frame):
    frame = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    frame = frame.dropna(subset=[f"{t.lower()}_{field}" for t in TICKERS for field in ["open", "close"]])
    frame = frame.reset_index(drop=True)
    features = {}
    for ticker in TICKERS:
        name = ticker.lower()
        close = frame[f"{name}_close"]
        open_price = frame[f"{name}_open"]
        features[f"{name}_daily_return"] = np.log(close).diff()
        features[f"{name}_intraday_return"] = np.log(close / open_price)

    lmt_returns = features["lmt_daily_return"]
    for window in [5, 20, 60]:
        features[f"lmt_momentum_{window}"] = lmt_returns.rolling(window).sum()
        features[f"lmt_volatility_{window}"] = lmt_returns.rolling(window).std()

    X = pd.DataFrame(features).replace([np.inf, -np.inf], np.nan)
    next_day_change = frame["lmt_close"].shift(-1) - frame["lmt_open"].shift(-1)
    valid = next_day_change.notna() & X.notna().all(axis=1)
    y = (next_day_change.loc[valid] > 0).astype(int)
    return frame.loc[valid, "date"], X.loc[valid], y


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2011-01-01", help="First download date")
    parser.add_argument("--csv", default="clean_data/final_data.csv", help="Existing prepared CSV")
    parser.add_argument("--download", action="store_true", help="Download longer history with yfinance")
    args = parser.parse_args()

    frame = download_history(args.start) if args.download else pd.read_csv(args.csv, parse_dates=["date"])
    dates, X, y = make_features(frame)
    train = dates < VAL_START
    val = (dates >= VAL_START) & (dates < TEST_START)
    test = (dates >= TEST_START) & (dates <= TEST_END)
    if min(train.sum(), val.sum(), test.sum()) < 30:
        raise ValueError("Not enough dates in one of the chronological splits")

    # Choose the model using validation data. The test set is evaluated once.
    candidates = {
        "Logistic regression": make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(),
            LogisticRegression(C=0.1, max_iter=1000),
        ),
        "Small boosted trees": HistGradientBoostingClassifier(
            max_iter=100, max_leaf_nodes=3, learning_rate=0.03,
            l2_regularization=10, random_state=42,
        ),
    }
    print(f"Days: train={train.sum()}, validation={val.sum()}, test={test.sum()}")
    print(f"Training starts: {dates.loc[train].iloc[0].date()}")
    best_name, best_model, best_val = None, None, -1.0
    for name, model in candidates.items():
        model.fit(X.loc[train], y.loc[train])
        score = accuracy_score(y.loc[val], model.predict(X.loc[val]))
        print(f"Validation accuracy, {name}: {score:.3f}")
        if score > best_val:
            best_name, best_model, best_val = name, model, score

    actual = y.loc[test]
    predicted = best_model.predict(X.loc[test])
    majority_class = int(y.loc[train].mean() >= 0.5)
    baseline = np.full(len(actual), majority_class)
    model_accuracy = accuracy_score(actual, predicted)
    baseline_accuracy = accuracy_score(actual, baseline)
    print(f"Selected on validation: {best_name}")
    print(f"Test accuracy: {model_accuracy:.3f}; balanced accuracy: {balanced_accuracy_score(actual, predicted):.3f}")
    print(f"Training-majority test accuracy on the same days: {baseline_accuracy:.3f}")
    print("Confusion matrix (actual rows 0/1, predicted columns 0/1):")
    print(confusion_matrix(actual, predicted, labels=[0, 1]))
    print("Beat baseline on test:", model_accuracy > baseline_accuracy)


if __name__ == "__main__":
    main()
