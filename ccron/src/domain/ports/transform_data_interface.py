from abc import ABC, abstractmethod

class TransformDataInterface(ABC):
    @abstractmethod
    def transformar_dados(self, dados: list[dict]) -> list[dict]:
        pass