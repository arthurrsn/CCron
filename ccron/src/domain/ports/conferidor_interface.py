from abc import ABC, abstractmethod
from typing import List, Dict, Tuple

class ConferidorInterface(ABC):
    @abstractmethod
    def get_servicos_simultaneos(dados: list[dict]) -> list:        
        pass
    
    @abstractmethod
    def get_macrofluxo(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def format_tabela_list_dict(self,
        lista_overlap: List[list], 
        lista_long_gaps: List[list], 
        dados_projeto: List[Dict]
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        pass