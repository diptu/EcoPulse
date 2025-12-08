import torch
from ml.datasets import EnergyDataset
from ml.lstm import LSTMModel


def train_lstm(csv_path):
    # Load and preprocess dataset
    dataset = EnergyDataset(csv_path)
    X_train, X_val, y_train, y_val = dataset.split()

    # Convert to tensors
    X_train = torch.tensor(X_train.values, dtype=torch.float32)
    y_train = torch.tensor(y_train.values, dtype=torch.float32).unsqueeze(1)
    X_val = torch.tensor(X_val.values, dtype=torch.float32)
    y_val = torch.tensor(y_val.values, dtype=torch.float32).unsqueeze(1)

    # Initialize LSTM model
    model = LSTMModel(input_dim=X_train.shape[1])
    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Training loop
    for epoch in range(10):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train)
        loss = criterion(outputs, y_train)
        loss.backward()
        optimizer.step()
        print(f"Epoch {epoch + 1}, Loss: {loss.item():.4f}")

    # Validation
    model.eval()
    with torch.no_grad():
        val_loss = criterion(model(X_val), y_val)
    print(f"Validation Loss: {val_loss.item():.4f}")

    return model


# ---- Hybrid prediction example ----
def hybrid_predict(model, vw_updater, feature_seq, feature_tabular):
    """
    model: trained PyTorch LSTM
    vw_updater: VWOnlineUpdater instance
    feature_seq: sequence input for LSTM [seq_len x features]
    feature_tabular: dict of features for VW incremental correction
    """
    # PyTorch prediction
    model.eval()
    with torch.no_grad():
        x = torch.tensor(feature_seq, dtype=torch.float32).unsqueeze(0)
        lstm_pred = model(x).item()

    # VW correction
    vw_pred = vw_updater.predict(feature_tabular)

    # Combine predictions (weighted average)
    alpha = 0.8  # weight for LSTM
    final_pred = alpha * lstm_pred + (1 - alpha) * vw_pred
    return final_pred
