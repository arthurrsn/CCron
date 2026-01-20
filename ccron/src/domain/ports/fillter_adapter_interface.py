from abc import ABC, abstractmethod
from typing import List, Dict

class FillterAdapterInterface(ABC):
    @abstractmethod
    def fill_forward(self, data: List[Dict], columns: List[str]) -> List[Dict]:
        pass

