import os
import json
import time
import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import ta

TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSM", "ASML", "AVGO",
    "JPM", "GS", "MS", "V", "MA", "BLK", "BX", "AXP", "C",
    "SONY", "SIEGY", "UPS", "FDX", "UNP", "CAT", "DE", "LMT", "GE",
    "LLY", "UNH", "JNJ", "WMT", "COST", "PG", "HD", "MCD", "NKE"
]

COLUMNS = [
    "Date", "Closing Price ($)", "Daily % Change", "Trading Volume", 
    "14-Day RSI (Momentum)", "Forward P/E (Valuation)", "Wall St Analyst Consensus"
]

def main():
    print("[Backfill Node] Initializing 1-Year Historical Sync...")
    
    # Authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ['SPREADSHEET_ID'])
    
    for ticker in TICKERS:
        try:
            print(f"[Backfill Node] Processing {ticker}...")
            stock = yf.Ticker(ticker)
            
            # Fetch 1 year of data (~252 trading days)
            hist = stock.history(period="1y")
            if hist.empty:
                continue
                
            # Calculate metrics
            hist['Daily Change'] = hist['Close'].pct_change() * 100
            hist['RSI'] = ta.momentum.RSIIndicator(hist['Close'], window=14).rsi()
            
            # Current valuation snapshots
            info = stock.info
            fwd_pe_raw = info.get('forwardPE', "N/A")
            fwd_pe = round(fwd_pe_raw, 2) if isinstance(fwd_pe_raw, (int, float)) else "N/A"
            consensus = info.get('recommendationKey', "N/A").replace('_', ' ').title()
            
            rows_to_insert = [COLUMNS]
            
            for i in range(len(hist)):
                date_str = hist.index[i].strftime('%Y-%m-%d')
                close_price = round(hist['Close'].iloc[i], 2)
                
                change_raw = hist['Daily Change'].iloc[i]
                daily_change = f"{round(change_raw, 2)}%" if pd.notna(change_raw) else "0.0%"
                
                vol = int(hist['Volume'].iloc[i])
                
                rsi_raw = hist['RSI'].iloc[i]
                rsi_val = round(rsi_raw, 2) if pd.notna(rsi_raw) else "N/A"
                
                # Apply real P/E and Consensus ONLY to the very last row (today's live data)
                is_last_row = (i == len(hist) - 1)
                pe_val = fwd_pe if is_last_row else "N/A"
                rating_val = consensus if is_last_row else "N/A"
                
                rows_to_insert.append([
                    date_str, close_price, daily_change, vol, rsi_val, pe_val, rating_val
                ])
                
            # Access and overwrite the tab
            try:
                worksheet = sheet.worksheet(ticker)
                worksheet.clear()  # Wipes the single test row
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=ticker, rows="1000", cols="10")
                
            # Bulk append avoids API limits
            worksheet.append_rows(rows_to_insert, value_input_option='USER_ENTERED')
            print(f"[Backfill Node] Successfully backfilled {len(rows_to_insert)-1} days for {ticker}.")
            
            # Strict pacing to stay under Google's 60 requests/minute ceiling
            time.sleep(2.5)
            
        except Exception as e:
            print(f"[Backfill Node] Error on {ticker}: {e}")
            
    print("[Backfill Node] 1-Year Historical Backfill Complete!")

if __name__ == "__main__":
    main()
  
