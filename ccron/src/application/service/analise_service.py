from ccron.src.domain.ports.analise_service_interface import AnaliseServiceInterface
from ccron.src.domain.ports.regras_validacao_interface import RegrasValidacaoInterface
from ccron.src.domain.service.regras_validacao import RegrasValidacao
from ccron.src.domain.ports.transform_data_interface import TransformDataInterface
from ccron.src.application.transform.transform_data import TransformData
from ccron.src.domain.ports.conferidor_interface import ConferidorInterface
from ccron.src.domain.service.conferidor import Conferidor

from datetime import datetime

class AnaliseService(AnaliseServiceInterface):
    def __init__(self):
        self.regras: RegrasValidacaoInterface = RegrasValidacao()
        self.transform_data: TransformDataInterface = TransformData()
        self.conferidor: ConferidorInterface = Conferidor()
        
    def filtrar_dados_ativos(self, dados: list[dict]) -> list[dict]:
        return [tarefa for tarefa in dados if tarefa.get('Ativo', '').strip().lower() == 'sim']

    def analisar_cronograma(self, dados: list[dict]) -> dict:
        self.dados_tratados = self.transform_data.transformar_dados(dados)
        self.dados_ativos = self.filtrar_dados_ativos(self.dados_tratados)


        self.lista_overlap, self.lista_gap = (
            self.conferidor.get_servicos_simultaneos(self.dados_ativos)
        )
        
        self.dados_peso_SAP = self.regras.validar_peso(self.dados_tratados)
        self.verificar_condicoes = self.regras.verificar_condicoes(self.dados_tratados)
        self.verificar_modulo = self.regras.verificar_modulo(self.dados_tratados)
        self.verificar_preenchimento = self.regras.verificar_preenchimento(self.dados_tratados)
        self.dados_regras_validacao = self.relatorio_project(self.dados_tratados)
        
        self.overlap, self.tabela_overlap, self.tabela_gap, self.gap = self.conferidor.format_tabela_list_dict(
            self.lista_overlap, self.lista_gap, self.dados_tratados)

        self.macrofluxo = self.conferidor.get_macrofluxo(self.dados_tratados)
        self.lista_coluna = self.lista_colunas()
     
        return {
            "dados_regras_validacao": self.dados_regras_validacao,
            "macrofluxo": self.macrofluxo,
            "dados_ativos": self.dados_ativos,
            "lista_overlap": self.lista_overlap,
            "lista_gap": self.lista_gap,
            "tabela_overlap": self.tabela_overlap,
            "tabela_gap": self.tabela_gap,
            "lista_colunas": self.lista_coluna
        }
        
    def relatorio_project(self, dados: list[dict]) -> list[dict]:
        hoje = datetime.today()
        
        dic_error = {}
        dic_error["ID tarefas sem predecessoras"] = self.regras.verificar_predecessoras(dados) #1
        dic_error["ID tarefas inativas com predecessoras"] = self.regras.verificar_id_inativo_com_predecessoras(dados)#2
        dic_error["ID tarefas atrasadas"] = self.regras.verificar_tarefas_atrasadas(dados, hoje)#3
        dic_error["ID tarefas com início real no futuro"] = self.regras.verificar_inicio_real_futuro(dados, hoje)#4
        dic_error["ID tarefas com término real no futuro"] = self.regras.verificar_termino_real_futuro(dados, hoje)#5
        dic_error["ID tarefas com tipo de duração inválido"] = self.regras.verificar_tipo_de_tarefas(dados)#6
        dic_error["ID tarefas com agendamento manual"] = self.regras.verificar_tarefas_com_agendamento_manual(dados)#7
        dic_error["ID tarefas com restrição"] = self.regras.tarefas_com_restricao(dados)#8
        dic_error["ID tarefas sem Recurso"] = self.regras.verificar_tarefas_com_duracao_e_sem_recurso(dados)#9
        dic_error["ID tarefas com trabalho incorreto"] = self.regras.verificar_duracao_por_dia(dados)#10    
        dic_error["ID tarefas inativas/resumo com recurso"] = self.regras.nao_deve_ter_recurso(dados)#11
        dic_error["ID tarefas ativas com custo zero"] = self.regras.tarefas_ativas_com_custo_zero(dados)#12
        dic_error["ID tarefas com duração zero"] = self.regras.tarefas_com_duracao_zero(dados)#13
        dic_error["ID tarefas com nome em branco"] = self.regras.tarefas_com_nome_em_branco(dados)#14
        dic_error["ID tarefas com usuario generico"] = self.regras.tarefas_com_recurso_usuario_generico(dados)#15
        dic_error["ID tarefas com duração maior que 21 dias"] = self.regras.tarefas_com_duracao_maior_que_21_dias(dados)#16
        dic_error["MO RATEIO, ANDAM JUNTO OU HABITE-SE estão ativas?"] = self.regras.verificar_tarefas_amp(dados)#17
        dic_error["ID com Peso 0"] = self.regras.obter_ids_com_peso_zero(dados)#20

        dic_error["Tarefas SAP com somatório de Peso diferente de 0"] = [item.get('SAP_Tarefa') for item in self.dados_peso_SAP]
        dic_error["ID tarefas com Agrupamentos Inconsistentes"] = self.verificar_condicoes
        dic_error["ID tarefas com Preenchimento Módulo ASC Inconsistentes"] = self.verificar_modulo
        
        for tipo in self.verificar_preenchimento:
            dic_error[f"ID tarefas com Ponto de Atenção no Preenchimento {tipo[0]}"] = tipo[1]
            
        dic_error["ID tarefas com Hiato"] = self.lista_gap
        dic_error["ID tarefas com Frente Simultânea"] = self.lista_overlap
        
        #dic_error["ID tarefas com latência maior que 5d"] = self.regras.tarefas_com_latencia(dados)#18
        #dic_error["ID tarefas com nível superior a 7"] = self.regras.tarefas_com_nivel_maior_que_7(dados)#19 
        
        return dic_error
        
    def lista_colunas(self):
        return [
            ["Id", "Nome", "Predecessoras", "Ativo"],#1
            ["Id", "Nome", "Ativo", "Predecessoras"],#2
            ["Id", "Nome", "Término", "Término_real"],#3
            ["Id", "Nome", "Início", "Início_real"],#4
            ["Id", "Nome", "Término", "Término_real"],#5
            ["Id", "Nome", "Número_da_estrutura_de_tópicos", "Tipo"],#6
            ["Id", "Nome", "Modo_da_Tarefa"],#7
            ["Id", "Nome", "Tipo_de_restrição"],#8
            ["Id", "Nome", "Nomes_dos_recursos"],#9
            ["Id", "Nome", "Duração", "Trabalho"],#10
            ["Id", "Nome", "Ativo", "Nomes_dos_recursos"], #11
            ["Id", "Nome", "Ativo", "Custo"], #12
            ["Id", "Nome", "Duração"], #13
            ["Id", "Nome"], #14
            ["Id", "Nome", "Ativo", "Nomes_dos_recursos"], #15
            ["Id", "Nome", "Duração"], #16
            ["Id", "Nome", "Ativo"], #17
            ["Id", "Nome", "Predecessoras"], #18
            ["Id", "Nome", "Nível_da_estrutura_de_tópicos"], #19
            ["Id", "Nome", "Peso"], #20

        ]
            
            
        
        