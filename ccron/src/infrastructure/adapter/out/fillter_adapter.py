from ccron.src.domain.ports.fillter_adapter_interface import FillterAdapterInterface

import pandas as pd
from typing import List, Dict, Any

class PandasFillterAdapter(FillterAdapterInterface):
    def fill_forward(self, data: List[Dict], columns: List[str]) -> List[Dict]:
        """
        Usa o pandas para aplicar a lógica de forward fill em colunas específicas.
        """
        if not data:
            return []
            
        df = pd.DataFrame(data)
        df[columns] = df[columns].fillna(method='ffill')
        
        # Converte valores <NA> do pandas para None do Python, garantindo compatibilidade
        return df.where(pd.notna(df), None).to_dict('records')