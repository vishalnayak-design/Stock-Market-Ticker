import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input
import logging

class Forecaster:
    def __init__(self, lookback=60):
        self.lookback = lookback
        self.model = None
        self.scaler = MinMaxScaler(feature_range=(0, 1))

    def create_dataset(self, dataset):
        """Converts array into X, y sequences."""
        X, y = [], []
        for i in range(len(dataset) - self.lookback):
            X.append(dataset[i : i + self.lookback, 0])
            y.append(dataset[i + self.lookback, 0])
        return np.array(X), np.array(y)

    def train_model(self, df):
        """Trains an LSTM model on the 'Close' price."""
        if len(df) < self.lookback + 20: 
            logging.warning("Not enough data to train LSTM")
            return None

        data = df.filter(['Close']).values
        scaled_data = self.scaler.fit_transform(data)

        X, y = self.create_dataset(scaled_data)
        X = np.reshape(X, (X.shape[0], X.shape[1], 1))

        # Build Model
        model = Sequential()
        model.add(Input(shape=(X.shape[1], 1)))
        model.add(LSTM(50, return_sequences=False))
        model.add(Dense(25))
        model.add(Dense(1))

        model.compile(optimizer='adam', loss='mean_squared_error')
        
        # Train
        model.fit(X, y, batch_size=16, epochs=5, verbose=0) # Low epochs for speed
        self.model = model
        return model

    def predict_next_days(self, df, days=7):
        """Predicts the next N days price trend."""
        if self.model is None:
            self.train_model(df)
            if self.model is None:
                return []

        data = df.filter(['Close']).values
        scaled_data = self.scaler.transform(data)
        
        # Start with the last lookback window
        last_window = scaled_data[-self.lookback:]
        current_batch = last_window.reshape((1, self.lookback, 1))
        
        predictions = []
        for _ in range(days):
            pred_sub = self.model.predict(current_batch, verbose=0)
            predictions.append(pred_sub[0, 0])
            
            # Update batch: remove first, add new prediction
            current_batch = np.append(current_batch[:, 1:, :], [[pred_sub[0]]], axis=1)

        # Inverse transform
        predictions = np.array(predictions).reshape(-1, 1)
        return self.scaler.inverse_transform(predictions).flatten()

    def get_forecast_score(self, df):
        """Simple strategy: 1 if forecast is bullish (next 7 days > current), else 0."""
        try:
            current_price = df['Close'].iloc[-1]
            preds = self.predict_next_days(df, days=5)
            if len(preds) == 0:
                return 0.5
            
            avg_pred = np.mean(preds)
            if avg_pred > current_price * 1.02: # >2% growth expected
                return 1
            elif avg_pred < current_price:
                return 0
            else:
                return 0.5
        except Exception as e:
            logging.error(f"Prediction error: {e}")
            return 0.5
