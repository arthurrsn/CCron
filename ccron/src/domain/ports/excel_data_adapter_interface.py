from abc import ABC, abstractmethod

class ExcelDataAdapterInterface(ABC):
    @abstractmethod
    def read_data(self) -> list[dict]:
        pass

