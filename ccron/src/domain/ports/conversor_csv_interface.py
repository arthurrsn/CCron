from abc import ABC, abstractmethod
from typing import List, Dict

class ConversorArquivoCsvInterface(ABC):
    @abstractmethod
    def csv_de_memoria_para_lista_dict(self, conteudo_bytes: bytes) -> List[Dict]:
        pass