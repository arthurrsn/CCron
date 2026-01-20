from abc import ABC, abstractmethod

class IntegracaoProjectAdapterInterface(ABC):
    @abstractmethod
    def pegar_projeto_mrv(self, project_id: str):
        pass
    
    @abstractmethod
    def buscar_dados_brutos_projeto(self, project_id: str):
        pass

    @abstractmethod
    def pegar_tarefas_project_data(self, project_id: str):
        pass