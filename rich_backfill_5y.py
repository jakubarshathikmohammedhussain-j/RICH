import os
import json
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def calculate_rsi_series(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)

def determine_rating(rsi, pct_change):
    if rsi <= 30:
        return "OVERSOLD_ACCUMULATE"
    elif rsi >= 70:
        return "OVERBOUGHT_DISTRIBUTE"
    elif pct_change >= 3.0:
        return "MOMENTUM_BULLISH"
    elif pct_change <= -3.0:
        return "MOMENTUM_BEARISH"
    return "NEUTRAL"

def main():
    print("[RICH BACKFILL] Initializing 5-Year Historical Data Pipeline...")
    
    # 1. BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.market_signals"

    # 2. Load Ticker Universe
    try:
        with open('master_tickers.txt', 'r') as f:
            target_tickers = [line.strip().upper() for line in f if line.strip()]
    except FileNotFoundError:
        print("[RICH ERROR] master_tickers.txt not found. Aborting.")
        return

    print(f"[RICH BACKFILL] Targeting {len(target_tickers)} tickers for 5-year historical backfill.")

    # 3. Batch Extraction & Calculation
    batch_size = 50
    all_records = []
    total_tickers = len(target_tickers)

    for i in range(0, total_tickers, batch_size):
        batch = target_tickers[i:i + batch_size]
        print(f"[RICH BACKFILL] Downloading batch {i + 1} to {min(i + batch_size, total_tickers)}...")
        
        try:
            # 5-Year extraction parameter
            df = yf.download(
                tickers=batch,
                period="5y",
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False
            )
            
            if df.empty:
                continue

            for ticker in batch:
                try:
                    if len(batch) > 1:
                        if ticker not in df.columns.levels[0]:
                            continue
                        sub_df = df[ticker].dropna(subset=['Close'])
                    else:
                        sub_df = df.dropna(subset=['Close'])

                    if len(sub_df) < 15:
                        continue

                    # Calculate continuous historical indicators
                    close_series = sub_df['Close']
                    pct_series = close_series.pct_change() * 100
                    rsi_series = calculate_rsi_series(close_series, period=14)
                    volume_series = sub_df['Volume'].fillna(0)

                    # Transform each trading day into a flat telemetry row
                    for dt, close_val in close_series.items():
                        prev_dt_pct = pct_series.get(dt, 0.0)
                        pct_val = 0.0 if pd.isna(prev_dt_pct) else round(float(prev_dt_pct), 2)
                        rsi_val = round(float(rsi_series.get(dt, 50.0)), 2)
                        vol_val = int(volume_series.get(dt, 0))
                        
                        all_records.append({
                            "timestamp": dt.strftime('%Y-%m-%dT00:00:00Z'),
                            "ticker": ticker,
                            "close_price": round(float(close_val), 2),
                            "percent_change": pct_val,
                            "volume": vol_val,
                            "rsi_14d": rsi_val,
                            "algorithmic_rating": determine_rating(rsi_val, pct_val)
                        })
                except Exception as e:
                    continue

        except Exception as e:
            print(f"[RICH ERROR] Batch processing failed: {e}")

    # 4. Stream to BigQuery in 50,000-Row Chunks
    total_records = len(all_records)
    print(f"[RICH BACKFILL] Extracted {total_records} historical rows. Ingesting to BigQuery...")

    chunk_size = 50000
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True
    )

    for idx in range(0, total_records, chunk_size):
        chunk = all_records[idx:idx + chunk_size]
        try:
            job = client.load_table_from_json(chunk, table_id, job_config=job_config)
            job.result()
            print(f"[RICH BACKFILL] Committed rows {idx + 1} to {min(idx + chunk_size, total_records)} to BigQuery.")
        except Exception as e:
            print(f"[RICH ERROR] Chunk upload failed at index {idx}: {e}")

    print("[RICH BACKFILL] Historical backfill complete.")

if __name__ == "__main__":
    main()
  
