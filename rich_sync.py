import os
import json
import time
import math
from datetime import datetime
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0.0).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)

def determine_rating(rsi, pct_change):
    if rsi < 35:
        return "Strong Buy"
    if rsi > 70:
        return "Overbought / Sell"
    if pct_change > 1.5:
        return "Bullish Momentum"
    if pct_change < -1.5:
        return "Bearish Trend"
    return "Hold / Neutral"

def stream_to_bigquery(client, table_id, rows_to_insert):
    if not rows_to_insert:
        return
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
        job.result()
        print(f"[BIGQUERY] Successfully batch-loaded {len(rows_to_insert)} market records.")
    except Exception as e:
        print(f"[BIGQUERY ERROR] {e}")

def main():
    print("[RICH Node] Initializing BigQuery global market sync...")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.market_signals"

    try:
        with open("master_tickers.txt", "r") as f:
            target_tickers = [line.strip().upper() for line in f if line.strip()]
    except FileNotFoundError:
        print("[RICH ERROR] master_tickers.txt not found. Aborting.")
        return

    print(f"[RICH] Loaded {len(target_tickers)} entities. Executing bulk extraction...")

    timestamp_iso = datetime.utcnow().isoformat()
    bq_payload = []
    email_results = []

    chunk_size = 500
    for i in range(0, len(target_tickers), chunk_size):
        chunk = target_tickers[i:i + chunk_size]
        try:
            print(f"[RICH] Fetching batch {i//chunk_size + 1} to {min(i+chunk_size, len(target_tickers))}...")
            data = yf.download(chunk, period="1mo", group_by="ticker", threads=True, progress=False)

            for ticker in chunk:
                try:
                    hist = data[ticker] if len(chunk) > 1 else data
                    hist = hist.dropna(subset=['Close'])
                    if hist.empty or len(hist) < 2:
                        continue

                    current_close = round(float(hist['Close'].iloc[-1]), 2)
                    prev_close = round(float(hist['Close'].iloc[-2]), 2)
                    volume = int(hist['Volume'].iloc[-1])

                    # Calculate changes
                    if prev_close == 0:
                        continue
                        
                    pct_change = round(((current_close - prev_close) / prev_close) * 100, 2)
                    rsi_val = calculate_rsi(hist['Close'])

                    # THE FIX: Sanitize NaN and Infinity values before BigQuery rejects them
                    if math.isnan(current_close) or math.isnan(pct_change) or math.isnan(rsi_val):
                        continue
                    if math.isinf(pct_change) or math.isinf(rsi_val):
                        continue

                    rating = determine_rating(rsi_val, pct_change)

                    bq_payload.append({
                        "timestamp": timestamp_iso,
                        "ticker": ticker,
                        "close_price": current_close,
                        "percent_change": pct_change,
                        "volume": volume,
                        "rsi_14d": rsi_val,
                        "algorithmic_rating": rating
                    })

                    email_results.append({
                        "ticker": ticker,
                        "close": current_close,
                        "change": pct_change,
                        "rsi": rsi_val,
                        "rating": rating
                    })
                except Exception:
                    continue
        except Exception as e:
            print(f"[RICH ERROR] Batch fetch failure: {e}")
            continue

    if bq_payload:
        stream_to_bigquery(client, table_id, bq_payload)

    sender_email = os.environ.get('GMAIL_USER')
    sender_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient_email = os.environ.get('RECIPIENT_EMAIL', sender_email)

    if not sender_email or not sender_password or len(email_results) == 0:
        return

    sorted_results = sorted(email_results, key=lambda x: x['change'], reverse=True)
    top_gainers = sorted_results[:5]
    top_laggards = sorted_results[-5:]

    msg = MIMEMultipart()
    msg['From'] = f"RICH Intelligence Node <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = f"RICH Market Intelligence: Global Sync Complete ({len(bq_payload)} entities)"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; color: #111; max-width: 600px; line-height: 1.5;">
        <h2 style="color: #0d1117; margin-bottom: 8px;">RICH Terminal: Global BigQuery Sync</h2>
        <p style="color: #586069; font-size: 14px; margin-top: 0;">Executed at {timestamp_iso} UTC</p>
        <p>Successfully processed <b>{len(bq_payload)}</b> global equities into the <b>market_signals</b> warehouse.</p>
        <hr style="border: 0; border-top: 1px solid #e1dfd5; margin: 16px 0;">
    """

    def generate_table(title, data, color):
        html = f"<h3 style='color: {color}; margin-bottom: 8px;'>{title}</h3>"
        html += "<table style='width: 100%; border-collapse: collapse; font-size: 13px;'>"
        html += "<tr style='background-color: #f6f8fa; text-align: left;'><th style='padding: 6px; border: 1px solid #d1d5db;'>Ticker</th><th style='padding: 6px; border: 1px solid #d1d5db;'>Close</th><th style='padding: 6px; border: 1px solid #d1d5db;'>Change %</th><th style='padding: 6px; border: 1px solid #d1d5db;'>14D RSI</th><th style='padding: 6px; border: 1px solid #d1d5db;'>Rating</th></tr>"
        for item in data:
            html += f"<tr><td style='padding: 6px; border: 1px solid #d1d5db;'><b>{item['ticker']}</b></td><td style='padding: 6px; border: 1px solid #d1d5db;'>${item['close']}</td><td style='padding: 6px; border: 1px solid #d1d5db;'>{item['change']}%</td><td style='padding: 6px; border: 1px solid #d1d5db;'>{item['rsi']}</td><td style='padding: 6px; border: 1px solid #d1d5db;'>{item['rating']}</td></tr>"
        html += "</table>"
        return html

    html_content += generate_table("Top Global Gainers", top_gainers, "#2ea44f")
    html_content += "<br>"
    html_content += generate_table("Top Global Laggards", top_laggards, "#cb2431")
    html_content += "</div>"

    msg.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"[RICH ERROR] Email dispatch failed: {e}")

if __name__ == "__main__":
    main()
    
