import requests
import logging

class Notifier:
    def __init__(self, config):
        self.bot_token = config['telegram'].get('bot_token')
        self.chat_id = config['telegram'].get('chat_id')

    def send_message(self, message):
        if not self.bot_token or not self.chat_id or self.bot_token == "YOUR_BOT_TOKEN":
            logging.warning("Telegram not configured.")
            return

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            requests.post(url, json=payload)
            logging.info("Sent Telegram alert.")
        except Exception as e:
            logging.error(f"Failed to send Telegram alert: {e}")

    def send_recommendation(self, data):
        # Convert to list of dicts if DataFrame
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            if data.empty:
                self.send_message("No SIP recommendations found for this month.")
                return
            records = data.to_dict('records')
        else:
            if not data:
                self.send_message("No SIP recommendations found for this month.")
                return
            records = data

        msg = "*🚀 Monthly SIP Recommendations*\n\n"
        for row in records:
            close_price = float(row.get('Close', 0))
            score = float(row.get('Final_Score', 0))
            allocation = float(row.get('Allocation', 0))
            
            msg += f"✅ *{row['Name']}* ({row['Ticker']})\n"
            msg += f"   💰 Price: ₹{close_price:.2f}\n"
            msg += f"   📊 Score: {score:.2f}\n"
            msg += f"   🛒 Buy Amount: ₹{allocation:.2f}\n\n"
        
        self.send_message(msg)
