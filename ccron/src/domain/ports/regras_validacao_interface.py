from abc import ABC, abstractmethod
import datetime

class RegrasValidacaoInterface(ABC):
    @abstractmethod
    def verificar_predecessoras(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_id_inativo_com_predecessoras(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def validar_peso(self, dados: list[dict]) -> list[dict]:
        pass
    
    @abstractmethod
    def verificar_tarefas_atrasadas(self, dados: list[dict], hoje: datetime) -> list[dict]:
        pass
    
    @abstractmethod
    def verificar_inicio_real_futuro(self, dados: list[dict], hoje: datetime) -> list[dict]:
        pass
    
    @abstractmethod
    def verificar_tarefas_com_agendamento_manual(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_termino_real_futuro(self, dados: list[dict], hoje: datetime) -> list:
        pass
    
    @abstractmethod
    def verificar_tipo_de_tarefas(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_restricao(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_tarefas_com_duracao_e_sem_recurso(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_duracao_por_dia(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def nao_deve_ter_recurso(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_ativas_com_custo_zero(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_duracao_zero(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_nome_em_branco(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_recurso_usuario_generico(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_duracao_maior_que_21_dias(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_tarefas_amp(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_latencia(self, task: list[dict]) -> list:
        pass
    
    @abstractmethod
    def tarefas_com_nivel_maior_que_7(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def obter_ids_com_peso_zero(self, dados: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_condicoes(self, tasks: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_modulo(self, tasks: list[dict]) -> list:
        pass
    
    @abstractmethod
    def verificar_preenchimento(self, tasks: list[dict]) -> list:
        pass