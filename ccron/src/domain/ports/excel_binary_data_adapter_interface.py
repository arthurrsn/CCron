from abc import ABC, abstractmethod
from typing import List, Dict

class ExcelDataAdapterInterface(ABC):
    @abstractmethod
    def read_data(self) -> List[Dict]:
        pass