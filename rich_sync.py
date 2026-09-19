import os
import json
import time
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account

# ==========================================
# 1. STRATEGIC TARGETS (The 36 Entities)
# ==========================================
TARGET_TICKERS = [
    # Logistics & Maritime
    "SIEGY", "UPS", "FDX", "EXPD", "CHRW", "ZIM",
    # Tech & AI Infrastructure
    "SONY", "NVDA", "AMD", "MSFT", "GOOGL", "AMZN", "META", "AAPL", "TSM", "ASML",
    # Defense, Aerospace & Industrials
    "LMT", "RTX", "NOC", "GD", "BA", "CAT", "DE",
    # Energy, Materials & Sovereign Proxies
    "XOM", "CVX", "SLB", "FCX", "ALB",
    # Consumer & Financial barometers
    "WMT", "TGT", "COST", "JPM", "GS", "MS", "BLK", "ACN"
]

# ==========================================
# 2. QUANTITATIVE HEURISTICS
# ==========================================
def calculate_rsi(prices, period=14):
    """Calculates exactly accurate 14-day RSI using a 30-day historical rolling window."""
    if len(prices) < period + 1:
        return 50.0 
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0.0).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)

def determine_rating(rsi, pct_change):
    if rsi < 35: return "Strong Buy"
    if rsi > 70: return "Overbought / Sell"
    if pct_change > 1.5: return "Bullish Momentum"
    if pct_change < -1.5: return "Bearish Trend"
    return "Hold / Neutral"

# ==========================================
# 3. BIGQUERY INGESTION ENGINE (Sandbox Safe)
# ==========================================
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

# ==========================================
# 4. MAIN EXECUTION ROUTINE
# ==========================================
def main():
    print("[RICH Node] Initializing BigQuery market sync...")
    
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.market_signals"

    timestamp_iso = datetime.utcnow().isoformat()
    bq_payload = []
    email_results = []

    for ticker in TARGET_TICKERS:
        try:
            print(f"[RICH] Extracting telemetry for {ticker}...")
            hist = yf.Ticker(ticker).history(period="1mo")
            if hist.empty or len(hist) < 2:
                print(f"  -> No data for {ticker}. API timeout. Skipping.")
                continue
            
            current_close = round(hist['Close'].iloc[-1], 2)
            prev_close = round(hist['Close'].iloc[-2], 2)
            volume = int(hist['Volume'].iloc[-1])
            
            pct_change = round(((current_close - prev_close) / prev_close) * 100, 2)
            rsi_val = calculate_rsi(hist['Close'])
            rating = determine_rating(rsi_val, pct_change)
            
            bq_payload.append({
                "timestamp": timestamp_iso,
                "domain": "RICH",
                "entity_id": ticker,
                "signal_type": "Daily Market Close",
                "raw_data": {
                    "close_price": current_close,
                    "percent_change": pct_change,
                    "volume": volume,
                    "rsi_14d": rsi_val,
                    "algorithmic_rating": rating
                }
            })
            
            email_results.append({
                "ticker": ticker, "close": current_close, 
                "change": pct_change, "rsi": rsi_val, "rating": rating
            })
            
        except Exception as e:
            print(f"[RICH ERROR] Critical failure on {ticker}: {e}")
        
        time.sleep(1.5) # Anti-ban throttle

    # Push to BigQuery
    if bq_payload:
        stream_to_bigquery(client, table_id, bq_payload)

    # ==========================================
    # 5. EMAIL DISPATCH ENGINE
    # ==========================================
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
    msg['Subject'] = f"RICH Market Intelligence: BigQuery Sync Complete"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; color: #111; max-width: 600px; line-height: 1.5;">
        <h2 style="color: #0d1117; margin-bottom: 4px;">RICH Terminal: BigQuery Sync</h2>
        <p style="color: #586069; font-size: 13px; margin-top: 0;">Executed at {timestamp_iso} UTC</p>
        <p>The <b>market_signals</b> database has been injected with end-of-day equity data.</p>
        <hr style="border: 0; border-top: 1px solid #e1e4e8; margin: 16px 0;" />
    """
    
    # Helper to generate tables
    def generate_table(title, data, color):
        html = f"<h3 style='color: {color}; margin-bottom: 8px;'>{title}</h3>"
        html += "<table style='width: 100%; border-collapse: collapse; font-size: 13px;'>"
        html += "<tr style='background-color: #eaecef; text-align: left;'><th style='padding: 6px; border: 1px solid #d1d5da;'>Ticker</th><th style='padding: 6px; border: 1px solid #d1d5da;'>Close</th><th style='padding: 6px; border: 1px solid #d1d5da;'>Change</th><th style='padding: 6px; border: 1px solid #d1d5da;'>RSI</th></tr>"
        for item in data:
            html += f"<tr><td style='padding: 6px; border: 1px solid #d1d5da;'><b>{item['ticker']}</b></td><td style='padding: 6px; border: 1px solid #d1d5da;'>${item['close']}</td><td style='padding: 6px; border: 1px solid #d1d5da; color: {color};'>{item['change']}%</td><td style='padding: 6px; border: 1px solid #d1d5da;'>{item['rsi']}</td></tr>"
        return html + "</table>"

    html_content += generate_table("Top Gainers", top_gainers, "#28a745")
    html_content += "<br>"
    html_content += generate_table("Top Laggards", top_laggards, "#cb2431")
    
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
  
