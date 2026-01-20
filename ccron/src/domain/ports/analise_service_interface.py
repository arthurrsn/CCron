from abc import ABC, abstractmethod

class AnaliseServiceInterface(ABC):
    @abstractmethod
    def analisar_cronograma(self, dados: list[dict]) -> dict:
        pass