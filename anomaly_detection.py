import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType

class Autoencoder(nn.Module):
    def __init__(self, input_dim, encoding_dim=10):
        super(Autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(True),
            nn.Linear(64, encoding_dim),
            nn.ReLU(True)
        )
        self.decoder = nn.Sequential(
            nn.Linear(encoding_dim, 64),
            nn.ReLU(True),
            nn.Linear(64, input_dim)
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded

class AutoencoderModel:
    def __init__(self, input_dim, model_path):
        self.model = Autoencoder(input_dim)
        self.model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        self.model.eval()

    def predict(self, batch_df: pd.DataFrame) -> pd.Series:
        data = batch_df.values.astype(np.float32)
        with torch.no_grad():
            inputs = torch.tensor(data)
            outputs = self.model(inputs)
            errors = torch.mean((outputs - inputs) ** 2, dim=1).numpy()
        return pd.Series(errors)

# Khởi tạo model 1 lần ngoài UDF để tránh load nhiều lần (tùy cách triển khai)
input_dim = 6
model_path = "model.pt"
ae_model = AutoencoderModel(input_dim, model_path)

@pandas_udf("double", PandasUDFType.SCALAR)
def predict_reconstruction_error(*cols):
    df = pd.concat(cols, axis=1)
    return ae_model.predict(df)
