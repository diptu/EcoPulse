import torch.nn as nn


class ANNModel(nn.Module):
    def __init__(self, input_dim, hidden_dim=64):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1),  # Predict kWh
        )

    def forward(self, x):
        return self.network(x)
        return self.network(x)
