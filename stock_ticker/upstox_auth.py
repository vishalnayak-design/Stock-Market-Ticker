import os
import requests
from dotenv import load_dotenv
import json
import webbrowser

# Load .env from project root
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI", "https://localhost:8501") # Default Streamlit port

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TOKEN_FILE = os.path.join(DATA_DIR, "upstox_token.json")

def login():
    if not API_KEY or not API_SECRET:
        print("❌ Error: UPSTOX_API_KEY or UPSTOX_API_SECRET not found in .env")
        print("Please create a .env file in the project root with:")
        print("UPSTOX_API_KEY=your_key\nUPSTOX_API_SECRET=your_secret\nUPSTOX_REDIRECT_URI=https://localhost:8501")
        return

    # 1. Generate Login URL
    login_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}"
    
    print("\n--- Upstox Login ---")
    print("1. Opening browser to authorize...")
    print(f"URL: {login_url}\n")
    
    try:
        webbrowser.open(login_url)
    except:
        print("(Could not open browser automatically. Please copy the URL above.)")
    
    # 2. Get Code
    code = input("2. After logging in, you will be redirected to an error page (localhost). \n   Copy the 'code' parameter from the URL bar (e.g., ...?code=xxxx&...)\n   Paste Code Here: ").strip()
    
    if not code:
        print("❌ No code provided.")
        return

    # 3. Exchange Code for Token
    token_url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "code": code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    
    try:
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()
        
        if "access_token" in token_data:
            # Save to file
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(TOKEN_FILE, "w") as f:
                json.dump(token_data, f, indent=4)
            print(f"\n✅ Success! Token saved to {TOKEN_FILE}")
            print(f"Access Token: {token_data['access_token'][:10]}...")
        else:
            print(f"❌ Failed to get token. Response: {token_data}")
            
    except Exception as e:
        print(f"❌ Error during token exchange: {e}")
        if 'response' in locals():
            print(response.text)

if __name__ == "__main__":
    login()
