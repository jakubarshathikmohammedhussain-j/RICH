import os
import json
import time
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. QUANTITATIVE HEURISTICS
# ==========================================
def calculate_rsi(prices, period=14):
    """Calculates exactly accurate 14-day RSI using a 30-day historical rolling window."""
    if len(prices) < period + 1:
        return 50.0 # Default fallback
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
# 2. MAIN EXECUTION ROUTINE
# ==========================================
def main():
    print("[RICH Node] Initializing autonomous daily market sync...")
    
    # 1. Connect to Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ['SPREADSHEET_ID'])

    # 2. Dynamically map all 36 Tickers from the tab names
    worksheets = sheet.worksheets()
    # Assume any tab with a short, uppercase name is a ticker (ignores "Dashboard" or "Macro" tabs)
    tickers = [ws.title for ws in worksheets if len(ws.title) <= 6 and ws.title.isupper()]
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    results = []

    # 3. Synchronize Data (Ring-fenced to prevent fatal crashes)
    for ticker in tickers:
        try:
            print(f"[RICH] Syncing telemetry for {ticker}...")
            ws = sheet.worksheet(ticker)
            
            # Pull 1 month of data to ensure mathematically perfect Prev Close and RSI
            hist = yf.Ticker(ticker).history(period="1mo")
            if hist.empty or len(hist) < 2:
                print(f"  -> No data returned for {ticker}. API may be delayed. Skipping.")
                continue
            
            # Extract absolute latest data
            current_close = round(hist['Close'].iloc[-1], 2)
            prev_close = round(hist['Close'].iloc[-2], 2)
            volume = int(hist['Volume'].iloc[-1])
            
            # Calculate metrics locally to fix "wrong value" bugs
            pct_change = round(((current_close - prev_close) / prev_close) * 100, 2)
            rsi_val = calculate_rsi(hist['Close'])
            pct_str = f"{pct_change}%" if pct_change <= 0 else f"+{pct_change}%"
            
            # Check to prevent duplicate logging for the same day
            existing_dates = ws.col_values(1)
            if existing_dates and existing_dates[-1] == today_str:
                print(f"  -> {ticker} already logged for {today_str}. Skipping duplicate.")
            else:
                new_row = [today_str, current_close, pct_str, volume, rsi_val]
                ws.append_row(new_row, value_input_option='USER_ENTERED')
                print(f"  -> Successfully logged: {current_close} ({pct_str})")
            
            # Add to briefing array
            results.append({
                "ticker": ticker,
                "close": current_close,
                "change": pct_change,
                "rsi": rsi_val,
                "rating": determine_rating(rsi_val, pct_change)
            })
            
        except Exception as e:
            # If one ticker fails, it prints the error and continues to the next one
            print(f"[RICH ERROR] Critical failure on {ticker}: {e}")
        
        # 4. Anti-Rate-Limit Throttle
        time.sleep(1.5)

    # ==========================================
    # 3. EMAIL DISPATCH ENGINE
    # ==========================================
    sender_email = os.environ.get('GMAIL_USER')
    sender_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient_email = os.environ.get('RECIPIENT_EMAIL', sender_email)

    if not sender_email or not sender_password or len(results) == 0:
        print("[RICH] Skipping email dispatch (no valid credentials or no data processed).")
        return

    # Sort for Gainers and Laggards
    sorted_results = sorted(results, key=lambda x: x['change'], reverse=True)
    top_gainers = sorted_results[:5]
    top_laggards = sorted_results[-5:]

    msg = MIMEMultipart()
    msg['From'] = f"RICH Intelligence Node <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = f"RICH Market Intelligence: {today_str} Summary"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; color: #111; max-width: 600px; line-height: 1.5;">
        <h2 style="color: #0d1117; margin-bottom: 4px;">RICH (Real-time Intrinsic Capital Heuristics)</h2>
        <p style="color: #586069; font-size: 13px; margin-top: 0;">Automated Daily Close Pipeline • {today_str}</p>
        <p>Your Google Sheet <b>RICH Database</b> has been synchronized with the latest market session data.</p>
        
        <h3 style="color: #28a745; margin-bottom: 8px;">Top Gainers</h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
            <tr style="background-color: #eaecef; text-align: left;">
                <th style="padding: 6px; border: 1px solid #d1d5da;">Ticker</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Close</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Change</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">RSI</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Analyst Rating</th>
            </tr>
    """
    for g in top_gainers:
        html_content += f"""
            <tr>
                <td style="padding: 6px; border: 1px solid #d1d5da;"><b>{g['ticker']}</b></td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">${g['close']}</td>
                <td style="padding: 6px; border: 1px solid #d1d5da; color: #28a745;">+{g['change']}%</td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">{g['rsi']}</td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">{g['rating']}</td>
            </tr>
        """
    
    html_content += """
        </table>
        <h3 style="color: #cb2431; margin-top: 20px; margin-bottom: 8px;">Top Laggards</h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
            <tr style="background-color: #eaecef; text-align: left;">
                <th style="padding: 6px; border: 1px solid #d1d5da;">Ticker</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Close</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Change</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">RSI</th>
                <th style="padding: 6px; border: 1px solid #d1d5da;">Analyst Rating</th>
            </tr>
    """
    for l in top_laggards:
        html_content += f"""
            <tr>
                <td style="padding: 6px; border: 1px solid #d1d5da;"><b>{l['ticker']}</b></td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">${l['close']}</td>
                <td style="padding: 6px; border: 1px solid #d1d5da; color: #cb2431;">{l['change']}%</td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">{l['rsi']}</td>
                <td style="padding: 6px; border: 1px solid #d1d5da;">{l['rating']}</td>
            </tr>
        """
        
    html_content += """
        </table>
        <hr style="border: 0; border-top: 1px solid #e1e4e8; margin: 20px 0;" />
        <p style="font-size: 11px; color: #6a737d;">Automated by HOLO_EARTH Central Command Node.</p>
    </div>
    """

    msg.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        print("[RICH] Executive briefing successfully dispatched.")
    except Exception as e:
        print(f"[RICH ERROR] Email dispatch failed: {e}")

if __name__ == "__main__":
    main()
    
