import pandas as pd
from sklearn.model_selection import train_test_split


class EnergyDataset:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.data = None

    def load_data(self):
        """Load raw CSV data"""
        self.data = pd.read_csv(self.csv_path)
        return self.data

    def preprocess(self):
        """Normalize and preprocess data"""
        df = self.data.copy()
        # Example: fill missing values
        df.fillna(0, inplace=True)
        # Feature engineering placeholder
        # df['hour_sin'] = np.sin(df['hour']*2*np.pi/24)
        return df

    def split(self, test_size=0.2, random_state=42):
        df = self.preprocess()
        X = df.drop(columns=["target_kwh"])  # Replace with actual target column
        y = df["target_kwh"]
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        return X_train, X_val, y_train, y_val
        return X_train, X_val, y_train, y_val
