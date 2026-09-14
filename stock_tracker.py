import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import ta

# 36 Top-Performing Global Leaders across 4 Core Pillars
TICKERS = [
    # 1. Big Tech & AI Infrastructure
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSM", "ASML", "AVGO",
    # 2. Global Finance & Private Capital
    "JPM", "GS", "MS", "V", "MA", "BLK", "BX", "AXP", "C",
    # 3. Industrial, Supply Chain & Logistics
    "SONY", "SIEGY", "UPS", "FDX", "UNP", "CAT", "DE", "LMT", "GE",
    # 4. Consumer Moats & Healthcare
    "LLY", "UNH", "JNJ", "WMT", "COST", "PG", "HD", "MCD", "NKE"
]

COLUMNS = [
    "Date",
    "Closing Price ($)",
    "Daily % Change",
    "Trading Volume",
    "14-Day RSI (Momentum)",
    "Forward P/E (Valuation)",
    "Wall St Analyst Consensus"
]

def send_summary_email(summary_data, recipient_email):
    sender_email = os.environ.get('GMAIL_USER')
    sender_password = os.environ.get('GMAIL_APP_PASSWORD')
    
    if not sender_email or not sender_password:
        print("[RICH Node] Missing email credentials in environment. Skipping email dispatch.")
        return

    # Sort to determine market movers
    valid_data = [x for x in summary_data if isinstance(x['change_pct'], (int, float))]
    valid_data.sort(key=lambda x: x['change_pct'], reverse=True)
    
    top_3 = valid_data[:3]
    bottom_3 = valid_data[-3:]

    msg = MIMEMultipart()
    msg['From'] = f"RICH System <{sender_email}>"
    msg['To'] = recipient_email
    today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    msg['Subject'] = f"RICH Market Intelligence: {today_str} Summary"

    def format_row(stock):
        color = "#2e7d32" if stock['change_pct'] >= 0 else "#c62828"
        sign = "+" if stock['change_pct'] > 0 else ""
        return f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><b>{stock['ticker']}</b></td>
            <td style="padding: 8px; border: 1px solid #ddd;">${stock['close']}</td>
            <td style="padding: 8px; border: 1px solid #ddd; color: {color}; font-weight: bold;">{sign}{stock['change_pct']}%</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{stock['rsi']}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{stock['consensus']}</td>
        </tr>
        """

    html_content = f"""
    <html>
      <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #202124; line-height: 1.5;">
        <h2 style="color: #0d47a1; margin-bottom: 4px;">RICH (Real-time Intrinsic Capital Heuristics)</h2>
        <p style="color: #5f6368; font-size: 14px; margin-top: 0;">Automated Daily Close Pipeline &bull; {today_str}</p>
        
        <p>Your Google Sheet <b>RICH Database</b> has been synchronized with the latest market session data.</p>
        
        <h3 style="color: #2e7d32; margin-top: 20px;">Top Gainers</h3>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px; font-size: 14px;">
          <thead>
            <tr style="background-color: #f1f3f4; text-align: left;">
              <th style="padding: 8px; border: 1px solid #ddd;">Ticker</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Close</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Change</th>
              <th style="padding: 8px; border: 1px solid #ddd;">RSI</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Analyst Rating</th>
            </tr>
          </thead>
          <tbody>
            {''.join([format_row(s) for s in top_3])}
          </tbody>
        </table>

        <h3 style="color: #c62828; margin-top: 24px;">Top Laggards</h3>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px; font-size: 14px;">
          <thead>
            <tr style="background-color: #f1f3f4; text-align: left;">
              <th style="padding: 8px; border: 1px solid #ddd;">Ticker</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Close</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Change</th>
              <th style="padding: 8px; border: 1px solid #ddd;">RSI</th>
              <th style="padding: 8px; border: 1px solid #ddd;">Analyst Rating</th>
            </tr>
          </thead>
          <tbody>
            {''.join([format_row(s) for s in bottom_3])}
          </tbody>
        </table>

        <br>
        <p style="font-size: 12px; color: #70757a; border-top: 1px solid #e0e0e0; padding-top: 12px;">
          Automated by HOLO_EARTH Central Command Node.
        </p>
      </body>
    </html>
    """
    msg.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        print("[RICH Node] Market briefing email successfully dispatched.")
    except Exception as e:
        print(f"[RICH Node] SMTP Error during transmission: {e}")

def main():
    print("[RICH Node] Starting execution...")
    today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    
    # 1. Establish Google Sheets Connection
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    spreadsheet_id = os.environ['SPREADSHEET_ID']
    sheet = client.open_by_key(spreadsheet_id)
    print(f"[RICH Node] Connected to spreadsheet: {sheet.title}")

    # Cache existing sheet titles to avoid redundant API lookup calls
    existing_worksheets = {ws.title: ws for ws in sheet.worksheets()}
    summary_data = []

    # 2. Iterate through Tickers
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1mo")
            
            if hist.empty or len(hist) < 2:
                print(f"[RICH Node] Insufficient price records for {ticker}, skipping.")
                continue
                
            close_price = round(hist['Close'].iloc[-1], 2)
            prev_close = round(hist['Close'].iloc[-2], 2)
            daily_change = round(((close_price - prev_close) / prev_close) * 100, 2)
            volume = int(hist['Volume'].iloc[-1])
            
            # Momentum: 14-Day RSI
            hist['RSI'] = ta.momentum.RSIIndicator(hist['Close'], window=14).rsi()
            rsi_raw = hist['RSI'].iloc[-1]
            rsi_val = round(rsi_raw, 2) if pd.notna(rsi_raw) else "N/A"
            
            # Fundamental Valuation & Wall Street Ratings
            info = stock.info
            fwd_pe_raw = info.get('forwardPE', "N/A")
            fwd_pe = round(fwd_pe_raw, 2) if isinstance(fwd_pe_raw, (int, float)) else "N/A"
            
            consensus = info.get('recommendationKey', "N/A").replace('_', ' ').title()

            row_data = [
                today_str,
                close_price,
                f"{daily_change}%",
                volume,
                rsi_val,
                fwd_pe,
                consensus
            ]
            
            # Ensure target worksheet exists
            if ticker in existing_worksheets:
                worksheet = existing_worksheets[ticker]
            else:
                print(f"[RICH Node] Creating new tab for {ticker}...")
                worksheet = sheet.add_worksheet(title=ticker, rows="1000", cols="10")
                worksheet.append_row(COLUMNS)
                existing_worksheets[ticker] = worksheet
                time.sleep(1.0)
            
            # Append current session values
            worksheet.append_row(row_data)
            print(f"[RICH Node] Synchronized {ticker} | Close: ${close_price} | Change: {daily_change}%")
            
            summary_data.append({
                "ticker": ticker,
                "close": close_price,
                "change_pct": daily_change,
                "rsi": rsi_val,
                "consensus": consensus
            })
            
            # Pacing delay to remain strictly under Google's 60 req/min threshold
            time.sleep(1.2)
            
        except Exception as e:
            print(f"[RICH Node] Error processing ticker {ticker}: {e}")
            continue

    # 3. Dispatch Intelligence Briefing
    recipient = os.environ.get('GMAIL_USER')
    if recipient and summary_data:
        send_summary_email(summary_data, recipient)
        
    print("[RICH Node] Execution cycle complete.")

if __name__ == "__main__":
    main()
  
