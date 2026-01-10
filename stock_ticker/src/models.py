import numpy as np
from sklearn.ensemble import RandomForestRegressor
import logging

class Forecaster:
    def __init__(self, lookback=60):
        self.lookback = lookback
        self.model = None

    def _extract_features(self, window):
        """Generates extended features from a price window."""
        # 1. Raw Window (Autoregression)
        features = list(window)
        
        # 2. Volatility (Standard Deviation) - Captures risk/noise
        volatility = np.std(window)
        features.append(volatility)
        
        # 3. Momentum (Rate of Change: Last vs First in window)
        momentum = (window[-1] - window[0]) / (window[0] + 1e-9)
        features.append(momentum)
        
        return np.array(features).reshape(1, -1)

    def prepare_data(self, history_list):
        """Creates training data with engineered features."""
        try:
            closes = [float(x['Close']) for x in history_list]
            if len(closes) < self.lookback + 5:
                return None, None
            
            X, y = [], []
            for i in range(len(closes) - self.lookback):
                window = np.array(closes[i : i + self.lookback])
                target = closes[i + self.lookback]
                
                # Extract features for this window
                # _extract_features returns (1, N), we need flat array for X list
                feats = self._extract_features(window)[0]
                
                X.append(feats)
                y.append(target)
            
            return np.array(X), np.array(y)
        except Exception as e:
            logging.error(f"Data prep error: {e}")
            return None, None

    def train_model(self, history_list):
        """Trains a Robust Random Forest."""
        X, y = self.prepare_data(history_list)
        if X is None or len(X) < 10:
            return None
            
        # Increased estimators for better accuracy (Stability)
        self.model = RandomForestRegressor(n_estimators=100, max_depth=15, random_state=42, n_jobs=-1)
        self.model.fit(X, y)
        return self.model

    def predict_next_days(self, history_list, days=7):
        """Recursive prediction with feature recalculation."""
        if self.model is None:
            self.train_model(history_list)
            if self.model is None:
                return []

        # Initial Window
        closes = [float(x['Close']) for x in history_list]
        current_window = np.array(closes[-self.lookback:])
        
        predictions = []
        for _ in range(days):
            # Generate features for current state
            feats = self._extract_features(current_window)
            
            # Predict
            pred = self.model.predict(feats)[0]
            predictions.append(pred)
            
            # Update window using numpy roll
            current_window = np.roll(current_window, -1)
            current_window[-1] = pred

        return predictions

    def get_forecast_score(self, history_list):
        """1 if bullish, 0 if bearish/neutral."""
        try:
            if not history_list:
                return 0.5
                
            current_price = float(history_list[-1]['Close'])
            preds = self.predict_next_days(history_list, days=5)
            
            if not preds:
                return 0.5
            
            avg_pred = np.mean(preds)
            
            if avg_pred > current_price * 1.02: 
                return 1.0
            elif avg_pred < current_price:
                return 0.0
            
            return 0.5
        except Exception as e:
            logging.error(f"Prediction error: {e}")
            return 0.5
