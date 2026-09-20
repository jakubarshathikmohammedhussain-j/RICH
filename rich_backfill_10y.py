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

def main():
    print("[RICH] Initializing 10-Year Equities & Global Macro Pipeline...")

    # 1. BigQuery Authentication
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.market_signals"

    # 2. Extract S&P 500 Tickers from Tier 1 Master Index
    query = f"SELECT DISTINCT ticker FROM `{creds_dict['project_id']}.telemetry_bronze.tier1_master_index`"
    try:
        sp500_df = client.query(query).to_dataframe()
        sp500_tickers = sp500_df['ticker'].dropna().unique().tolist()
        print(f"[RICH] Loaded {len(sp500_tickers)} S&P 500 tickers from tier1_master_index.")
    except Exception as e:
        print(f"[RICH ERROR] Failed reading tier1_master_index: {e}")
        return

    # 3. Add Tier 2 Global Macro & International Leaders
    global_macro_tickers = [
        # Nifty 50 Core Leaders (India)
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        # European Champions
        "ASML.AS", "SAP.DE", "NOVO-B.CO", "MC.PA", "AIR.PA",
        # Global Mining, Shipping & Raw Materials
        "BHP.L", "RIO.L", "VALE", "MAERSK-B.CO", "HLAG.DE"
    ]

    # Combine universes without duplicates
    full_universe = sorted(list(set(sp500_tickers + global_macro_tickers)))
    print(f"[RICH] Full combined universe: {len(full_universe)} global equities.")

    # 4. Batch Download to avoid connection timeouts (chunks of 60)
    chunk_size = 60
    chunks = [full_universe[i:i + chunk_size] for i in range(0, len(full_universe), chunk_size)]
    
    all_records = []
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        autodetect=True
    )

    for idx, chunk in enumerate(chunks):
        print(f"[RICH] Downloading batch {idx + 1}/{len(chunks)} ({len(chunk)} assets)...")
        try:
            df = yf.download(
                tickers=chunk,
                period="10y",
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False
            )
        except Exception as e:
            print(f"[RICH ERROR] Batch {idx + 1} download failed: {e}")
            continue

        if df.empty:
            continue

        for ticker in chunk:
            try:
                if len(chunk) == 1:
                    sub_df = df.dropna(subset=['Close'])
                else:
                    if ticker not in df.columns.levels[0]:
                        continue
                    sub_df = df[ticker].dropna(subset=['Close'])

                if len(sub_df) < 15:
                    continue

                close_series = sub_df['Close']
                pct_series = close_series.pct_change() * 100
                rsi_series = calculate_rsi_series(close_series, period=14)
                vol_series = sub_df['Volume'].fillna(0)

                signal_tag = "GLOBAL_MACRO_EOD" if ticker in global_macro_tickers else "EQUITY_EOD"

                for dt, close_val in close_series.items():
                    pct_val = pct_series.get(dt, 0.0)
                    rsi_val = rsi_series.get(dt, 50.0)

                    all_records.append({
                        "timestamp": dt.strftime('%Y-%m-%dT00:00:00Z'),
                        "domain": "RICH",
                        "entity_id": ticker,
                        "close_price": round(float(close_val), 2),
                        "percent_change": 0.0 if pd.isna(pct_val) else round(float(pct_val), 2),
                        "volume": int(vol_series.get(dt, 0)),
                        "rsi_14d": 50.0 if pd.isna(rsi_val) else round(float(rsi_val), 2),
                        "signal_type": signal_tag
                    })

            except Exception as e:
                print(f"[RICH ERROR] Parsing error on {ticker}: {e}")
                continue

        # Ingest every 40,000 records to keep runner RAM low
        if len(all_records) >= 40000:
            try:
                client.load_table_from_json(all_records, table_id, job_config=job_config).result()
                print(f"[RICH] Successfully streamed batch of {len(all_records)} rows.")
                all_records = []
            except Exception as e:
                print(f"[RICH ERROR] BigQuery batch write error: {e}")

    # Final batch flush
    if all_records:
        try:
            client.load_table_from_json(all_records, table_id, job_config=job_config).result()
            print(f"[RICH] Successfully streamed final {len(all_records)} rows.")
        except Exception as e:
            print(f"[RICH ERROR] BigQuery final write error: {e}")

    print("[RICH] 10-Year Global Equities backfill completed successfully.")

if __name__ == "__main__":
    main()
    
