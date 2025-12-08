import torch.nn as nn


class LSTMModel(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        output, (hn, cn) = self.lstm(x)
        return self.fc(output[:, -1, :])  # Use last timestep
        return self.fc(output[:, -1, :])  # Use last timestep
