from ccron.src.domain.ports.regras_validacao_interface import RegrasValidacaoInterface
from datetime import datetime
import re

class RegrasValidacao(RegrasValidacaoInterface):
    def verificar_predecessoras(self, dados: list[dict]) -> list:
        """
        Aponta para tarefas que não possuem predecessoras no cronograma.
        
        Mesmo tarefas com restrições de data, é importante que tenham predecessoras,
        com exceção da primeira tarefa do projeto.

        Retorna uma lista de IDs das tarefas que não têm predecessoras.
        """
        return [
            item.get("Id")
            for item in dados
            if (item.get("Resumo") == "Não")
            and (item.get("Ativo") == "Sim")
            and (item.get("Predecessoras") == "nan" or (not item.get("Predecessoras")))
        ]
        
    def verificar_id_inativo_com_predecessoras(self, dados: list[dict]) -> list:
        return [
            item.get("Id")
            for item in dados
            if (item.get("Resumo") == "Não")
            and (item.get("Ativo") == "Não")
            and item.get("Predecessoras") and item.get("Predecessoras") != "nan"
        ]
    
    def validar_peso(self, dados: list[dict]) -> list:
        """
        Valida se o somatório de pesos de tarefas filhas é 100% para um serviço ou pavimento.

        Quando uma atividade possui mais de um serviço abaixo dela, ou um serviço
        possui divisões em pavimentos, a somatória dos pesos de suas "filhas"
        deve ser igual a 100 no total. A validação agrupa as tarefas e
        verifica se o somatório do peso para cada grupo é diferente de 100 e também
        diferente de zero.

        Args:
            dados (list[dict]): A lista de tarefas a ser validada.

        Returns:
            list[dict]: Uma lista de dicionários representando os grupos com
            somatório de peso inválido. Cada dicionário contém as chaves de
            agrupamento (Aux, SAP_Tarefa, ID_Bloco), o somatório do peso e o
            ID da primeira tarefa encontrada no grupo.
        """
        grupos = {}

        def cortar_estrutura(numero: str | None) -> str:
            if not numero: return ""
            partes = str(numero).split(".")
            return "".join(partes[:-1])

        for item in dados:
            aux = cortar_estrutura(item.get("Número_da_estrutura_de_tópicos"))
            chave = (aux, item.get('SAP_Tarefa'), item.get('ID_Bloco'))
            
            if not all(chave): continue

            if chave not in grupos:
                grupos[chave] = {'Peso': 0.0, 'IdTarefa': None}
            
            try:
                grupos[chave]['Peso'] += float(item.get('Peso', 0.0))
            except (ValueError, TypeError):
                pass  # Ignora pesos não numéricos
                
            if grupos[chave]['IdTarefa'] is None:
                grupos[chave]['IdTarefa'] = item.get('Id')

        resultado_final = []
        for chave, valores in grupos.items():
            if valores['Peso'] != 100 and valores['Peso'] != 0:
                resultado_final.append({
                    "Aux": chave[0],
                    "SAP_Tarefa": chave[1],
                    "ID_Bloco": chave[2],
                    "Peso": valores['Peso'],
                    "IdTarefa": valores['IdTarefa']
                })
                
        return resultado_final
    
    def verificar_tarefas_atrasadas(self, dados: list[dict], hoje: datetime) -> list:
        """
        Verifica tarefas ativas e não concluídas cuja data de término agendada já passou.

        Este indicador aponta para tarefas ativas que não têm data de término real
        preenchida e cuja data de término agendada é igual ou anterior à data atual.

        Args:
            dados (list[dict]): A lista de tarefas a ser verificada.
            hoje: A data atual para comparação.

        Returns:
            list: Uma lista de IDs das tarefas atrasadas.
        """
        return [
            item["Id"]
            for item in dados
            if (item.get("Término") and str(item.get("Término")).lower() != "nan") 
            and (item.get("Ativo") == "Sim")
            and (not item.get("Término_real") or str(item.get("Término_real")).lower() == "nan")
            and (datetime.strptime(item.get("Término"), r"%d/%m/%Y") <= hoje)
        ]
        
    def verificar_inicio_real_futuro(self, dados: list[dict], hoje: datetime) -> list:
        """
        Verifica tarefas que possuem uma data de início real no futuro.

        Tarefas com o campo "Início real" preenchido já devem ter sido iniciadas.
        Este apontamento é útil para identificar possíveis erros no lançamento de dados.
        A única exceção é se a conferência for feita após a semana em que se projeta
        o realizado do final do mês, pois nesse caso é comum que algumas tarefas tenham
        início real à frente da data da conferência.

        Args:
            dados (list[dict]): A lista de tarefas a ser verificada.
            hoje: A data atual para comparação.

        Returns:
            list: Uma lista de IDs das tarefas que se encaixam na condição.
        """
        return [
            item["Id"]
            for item in dados
            if (item.get("Início_real") and str(item.get("Início_real")).lower() != "nan")  
            and datetime.strptime(item.get("Início_real"), r"%d/%m/%Y") > hoje
        ]
    
    def verificar_tarefas_com_agendamento_manual(self, dados: list[dict]) -> list:
        """
        Identifica tarefas que não estão configuradas com "Agendamento Automático".

        O Conferidor de Projetos aponta tarefas que possuem a coluna Modo da Tarefa
        selecionada com a opção "Agendada Manualmente", uma vez que é obrigatório
        o Agendamento Automático.

        Retorna uma lista de IDs das tarefas que possuem agendamento manual.
        """
        return [
            item["Id"]
            for item in dados
            if item.get("Modo_da_Tarefa") != "Agendada Automaticamente"
        ]
    
    def verificar_termino_real_futuro(self, dados: list[dict], hoje: datetime) -> list:
        """
        Verifica tarefas que possuem uma data de término real no futuro.

        Esta validação é útil para identificar possíveis erros no lançamento de dados.
        A única exceção para não correção é se o conferidor for utilizado após
        a semana em que se projeta o realizado no final do mês, pois nesse
        caso, algumas tarefas podem ter sido finalizadas à frente da data da
        conferência.

        Args:
            dados (list[dict]): A lista de tarefas a ser verificada.
            hoje: A data atual para comparação.

        Returns:
            list: Uma lista de IDs das tarefas que se encaixam na condição.
        """
        return [
            item["Id"]
            for item in dados
            if (item.get("Término_real") and str(item.get("Término_real")).lower() != "nan")
            and datetime.strptime(item.get("Término_real"), r"%d/%m/%Y") > hoje
        ]

    def verificar_tipo_de_tarefas(self, dados: list[dict]) -> list:
        """
        Verifica tarefas ativas e não-resumo com um tipo de duração diferente de "Trabalho Fixo".

        Tarefas com tipo de duração diferente de Trabalho Fixo são consideradas
        inválidas, e a correção é obrigatória.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        return [
            item["Id"]
            for item in dados
            if item.get("Tipo") != "Trabalho fixo"
            and item.get("Resumo") == "Não"
            and item.get("Ativo") == "Sim"
        ]
        
    def tarefas_com_restricao(self, dados: list[dict]) -> list:
        """
        Verifica tarefas ativas com restrições de data, ou seja, aquelas que
        tiveram sua data de início imputada manualmente.

        Este tipo de restrição é possível, mas a boa prática é evitá-lo.
        A correção não é obrigatória, mas serve como ponto de atenção para o planejador.
        A função aponta tarefas com restrições diferentes de 'O Mais Breve Possível'.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        return [
            item["Id"]
            for item in dados
            if item.get("Tipo_de_restrição") != "O Mais Breve Possível"
            and item.get("Ativo") == "Sim"
        ]
        
        
    def verificar_tarefas_com_duracao_e_sem_recurso(self, dados: list[dict]) -> list:
        """
        Verifica tarefas ativas, não-resumo e com duração maior que zero, mas que
        não possuem nenhum recurso alocado.

        O Conferidor aponta tarefas sem preenchimento na coluna "Nomes dos
        recursos". Esta verificação se aplica a tarefas no último nível do projeto.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        return [
            item["Id"]
            for item in dados
            if item.get("Duração", 0) > 0
            and item.get("Resumo") == "Não"
            and item.get("Ativo") == "Sim"
            and ((not item.get("Nomes_dos_recursos")) or str(item.get("Nomes_dos_recursos")).lower() == "nan")
        ]
        
    def verificar_duracao_por_dia(self, dados: list[dict]) -> list:
        """
        Verifica se a coluna 'Trabalho' está preenchida incorretamente.

        O Conferidor de Projetos aponta tarefas onde o valor da coluna "Trabalho"
        está incorreto, ou seja, diferente do cálculo de duração em dias vezes 8 horas,
        que é a jornada de trabalho considerada pelo Project.

        Args:
            dados (list[dict]): A lista de tarefas a ser verificada.

        Returns:
            list: Uma lista de IDs das tarefas que se encaixam na condição de
            terem o trabalho incorreto.
        """
        ids_invalidos = []
        for item in dados:
            duracao = item.get("Duração")
            trabalho = item.get("Trabalho")

            if (duracao is not None or str(duracao).lower() != "nan") and duracao >= 0 and item.get("Resumo") == "Não" and item.get("Ativo") == "Sim":
                trabalho_int = int(trabalho) if trabalho is not None else 0
                duracao_int = int(duracao) if duracao is not None else 0

                if duracao_int > 0 and (trabalho_int / 8) != duracao_int:
                    ids_invalidos.append(item["Id"])
        return ids_invalidos
    
    def nao_deve_ter_recurso(self, dados: list[dict]) -> list:
        """
        Verifica tarefas que não deveriam ter recursos alocados, mas que os possuem.

        Tarefas resumo e tarefas inativas não devem ter preenchimento no campo
        "Nomes dos Recursos".

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        ids_invalidos = []
        for item in dados:
            if item.get("Nomes_dos_recursos") and str(item.get("Nomes_dos_recursos")).lower() != "nan":
                if item.get("Resumo") == "Sim" or item.get("Ativo") == "Não":
                    ids_invalidos.append(item["Id"])
        return ids_invalidos
    
    def tarefas_ativas_com_custo_zero(self, dados: list[dict]) -> list:
        """
        Verifica tarefas ativas com custo zero.

        Obrigadoriamente, toda tarefa ativa deve apresentar algum custo,
        com exceção das tarefas marco, que possuem duração zero.

        Retorna uma lista de IDs das tarefas ativas que não possuem custo.
        """
        return [item["Id"] for item in dados if item.get("Ativo") == "Sim" and item.get("Custo") == 0]
    
    def tarefas_com_duracao_zero(self, dados: list[dict]) -> list:
        """
        Verifica tarefas com duração igual a zero.
        
        Tarefas com duração igual a zero consequentemente ficam com o trabalho zerado
        e não agregam na soma do AMP. A correção deste apontamento é obrigatória.
        
        Retorna uma lista de IDs das tarefas com duração zero.
        """
        return [item["Id"] for item in dados if item.get("Duração") == 0]
    
    def tarefas_com_nome_em_branco(self, dados: list[dict]) -> list:
        """
        Verifica tarefas ativas com ID diferente de 0 que não têm nome.

        Tarefas com nome em branco podem ser causadas por erros do planejador
        ou por uma inserção incorreta de linhas no cronograma. A inclusão
        de linhas extras pode gerar N linhas no intervalo e até mesmo erros no
        arquivo CSV, impedindo a geração de relatórios em alguns casos.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        return [
            item["Id"]
            for item in dados
            if (not item.get("Nome") or str(item.get("Nome")).lower() == "nan")
            and item.get("Id") != 0
            and item.get("Ativo") == "Sim"
        ]
        
    def tarefas_com_recurso_usuario_generico(self, dados: list[dict]) -> list:
        """
        Verifica se o recurso 'Usuário Genérico' foi alocado para alguma tarefa.
        
        A função aponta para tarefas onde a coluna "Nomes dos recursos" está
        preenchida com "Usuário Genérico".

        Retorna uma lista de IDs das tarefas que se encaixam nessa condição.
        """
        return [item["Id"] for item in dados if item.get("Nomes_dos_recursos") == "Usuário Genérico"]

    def tarefas_com_duracao_maior_que_21_dias(self, dados: list[dict]) -> list:
        """
        Verifica tarefas que não são de resumo e têm duração superior a 21 dias.

        O Conferidor de Projetos aponta tarefas com mais de 21 dias de duração,
        pois a diretriz para um bom planejamento é que as tarefas mais longas
        sejam divididas em etapas para terem no máximo duas semanas de duração.
        A correção desse apontamento não é obrigatória, mas serve como ponto de
        atenção.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        return [
            item["Id"]
            for item in dados
            if item.get("Duração", 0) > 21.0 and item.get("Resumo") == "Não"
        ]
    
    def verificar_tarefas_amp(self, dados: list[dict]) -> list:
        """
        Verifica se tarefas com nomes específicos estão ativas.

        Tarefas como "MÃO DE OBRA RATEIO", "ANDAM JUNTO" ou "HABITE-SE"
        devem estar inativadas no Project, pois são mensuradas de acordo com
        o AMP do projeto.

        Retorna uma lista de IDs das tarefas ativas que se encaixam nessa condição.
        """
        tarefas_alvo = ["ANDAM JUNTO", "MÃO DE OBRA RATEIO", "HABITE-SE"]
        return [
            item["Id"]
            for item in dados
            if item.get("Nome") in str(tarefas_alvo).upper() and item.get("Ativo") == "Sim"
        ]
        
    def tarefas_com_latencia(self, dados: list[dict]) -> list:
        """
        Encontra tarefas com latência de predecessora maior que 5 dias.
        
        Args:
            tasks: Uma lista de dicionários, onde cada dicionário é uma tarefa.
        
        Returns:
            Uma lista com os IDs das tarefas que atendem ao critério.
        """
        result = []
        for item in dados:
            try:
                latencia = re.search(r'.*\s?(\+|-)\s?(\d*)', item.get("Predecessoras"))
                if latencia and int(latencia.group(2)) > 5:
                    result.append(item.get("Id"))                
            except Exception:
                pass
        return result
    
    def tarefas_com_nivel_maior_que_7(self, dados: list[dict]) -> list:
        """
        Verifica se o nível da tarefa na estrutura é maior que 7.

        Tarefas com Nível de estrutura de tópicos superior a 7 não devem existir.

        Retorna uma lista de IDs das tarefas que se encaixam nessa condição.
        """
        return [
            item["Id"] 
            for item in dados 
            if item.get("Nível_da_estrutura_de_tópicos", 0) > 7]
    
    def obter_ids_com_peso_zero(self, dados: list[dict]) -> list[str]:
        """
        Verifica tarefas ativas e não-resumo com peso zero, que não sejam exceções.

        A função identifica tarefas com Peso == 0 que não deveriam ter, com base
        em uma lista de palavras-chave de exclusão (ex: "iptu", "itbi", "projeto").
        Se o nome da tarefa não contiver nenhuma dessas palavras, ela é sinalizada.

        Args:
            dados (list[dict]): A lista de tarefas a ser verificada.

        Returns:
            list[str]: Uma lista de IDs das tarefas que se encaixam na condição.
        """
        palavras_excluidas = {
            "leg ", "iptu", "itbi", "escritura", "inc ", 
            "projeto", "custo material pp - obra"
        }
        return [
            item["Id"]
            for item in dados
            if (item.get("Peso") == 0 and
                item.get("Resumo") == "Não" and
                item.get("Ativo") == "Sim" and
                # A linha abaixo executa a verificação das palavras excluídas
                not any(palavra in str(item.get("Nome", "")).lower() for palavra in palavras_excluidas))
        ]
    
    def verificar_condicoes(self, tasks: list[dict]) -> list:
        """
        Identifica tarefas com agrupamentos inconsistentes, ou seja, preenchidos
        de maneira diferente do padrão estabelecido.

        Esta validação é complexa e envolve a verificação de diversas condições
        em diferentes níveis da estrutura de tópicos, agrupamentos e tipos de projeto
        (Blocos ASC, Pré-Projeto e outros). A lógica da função é dividida para
        tratar cada cenário de forma específica.

        Returns:
            list: Uma lista de IDs das tarefas que se encaixam na condição de
            terem agrupamentos inconsistentes.
        """
        def _verificar_condicoes_helper(lista_tarefas: list[dict], niveis: list, agrupamento_incorreto: str) -> list:
            return [
                t["Id"]
                for t in lista_tarefas
                if t.get("Nível_da_estrutura_de_tópicos") in niveis
                and t.get("Agrupamento") != agrupamento_incorreto
            ]

        def _obter_numero_estrutura_helper(lista_tarefas: list[dict], nome_tarefa: str) -> str | None:
            for t in lista_tarefas:
                if t.get("Nome") == nome_tarefa:
                    return t.get("Número_da_estrutura_de_tópicos")
            return None

        def _verificar_agrupamento_helper(lista_tarefas: list[dict], nome_tarefa: str, agrupamento_esperado: str) -> list:
            return [
                t["Id"]
                for t in lista_tarefas
                if t.get("Nome") == nome_tarefa
                and t.get("Agrupamento") != agrupamento_esperado
            ]
            
        tarefas_processadas = []
        for t in tasks:
            copia_task = t.copy()
            copia_task["Nome"] = str(t.get("Nome", "")).strip().lower()
            copia_task["Agrupamento"] = str(t.get("Agrupamento", "")).strip().lower()
            tarefas_processadas.append(copia_task)

        condicoes_invalidas = {}

        lista_asc = [
            t
            for t in tarefas_processadas
            if str(t.get("Número_da_estrutura_de_tópicos", "")).startswith("1.1.1.")
        ]
        condicoes_invalidas["ASC Nível 3"] = _verificar_condicoes_helper(lista_asc, [3], "bloco asc")
        condicoes_invalidas["ASC Nível 4"] = _verificar_condicoes_helper(lista_asc, [4], "diagrama de rede")
        condicoes_invalidas["ASC Nível 5"] = _verificar_condicoes_helper(lista_asc, [5], "asc")
        condicoes_invalidas["ASC Nível 6"] = _verificar_condicoes_helper(lista_asc, [6], "servico")

        numero_pp = _obter_numero_estrutura_helper(tarefas_processadas, "pré projeto - pp")
        if numero_pp:
            lista_pp = [
                t
                for t in tarefas_processadas
                if str(t.get("Número_da_estrutura_de_tópicos", "")).startswith(numero_pp)
            ]
            condicoes_invalidas["PP Níveis 3-6"] = _verificar_condicoes_helper(lista_pp, [3, 4, 5, 6], "pré projeto")
            condicoes_invalidas["PP Nível 7"] = _verificar_condicoes_helper(lista_pp, [7], "pré projeto módulo")

        condicoes_invalidas["Agrup. Habite-se"] = _verificar_agrupamento_helper(tarefas_processadas, "habite-se", "habite-se")
        condicoes_invalidas["Agrup. Mão de Obra"] = _verificar_agrupamento_helper(tarefas_processadas, "mão de obra rateio", "mão de obra rateio")
        
        todos_numeros_estrutura = {t.get("Número_da_estrutura_de_tópicos") for t in tarefas_processadas if t.get("Número_da_estrutura_de_tópicos")}
        
        numeros_estrutura_outros = {
            num
            for num in todos_numeros_estrutura
            if len(num) >= 5
            and not num.startswith("1.1.1.")
            and not (numero_pp and num.startswith(numero_pp))
        }
        
        lista_outros = [
            t
            for t in tarefas_processadas
            if t.get("Número_da_estrutura_de_tópicos") in numeros_estrutura_outros
            and t.get("Nome") not in ["habite-se", "mão de obra rateio"]
        ]
        
        condicoes_invalidas["Outros Nível 3"] = _verificar_condicoes_helper(lista_outros, [3], "bloco")
        condicoes_invalidas["Outros Nível 4"] = _verificar_condicoes_helper(lista_outros, [4], "diagrama de rede")
        condicoes_invalidas["Outros Nível 5"] = _verificar_condicoes_helper(lista_outros, [5], "bloco (infra/supra)")
        condicoes_invalidas["Outros Nível 6"] = _verificar_condicoes_helper(lista_outros, [6], "servico")
        condicoes_invalidas["Outros Nível 7"] = _verificar_condicoes_helper(lista_outros, [7], "pavimento")
        
        ids_finais = set()
        for lista_de_ids in condicoes_invalidas.values():
            for id_task in lista_de_ids:
                ids_finais.add(id_task)
                
        return list(ids_finais)
    
    def verificar_modulo(self, tasks: list[dict]) -> list:
        """
        Verifica o preenchimento do campo MÓDULO_ASC em empreendimentos
        que possuem mais de um módulo.

        O Conferidor identifica a falta de preenchimento na coluna MÓDULO_ASC
        em tarefas onde ele é necessário, como em Bloco ASC e Pré-Projeto.

        Retorna uma lista de IDs das tarefas que se encaixam nessas condições.
        """
        tarefas_ativas = [t for t in tasks if t.get("Ativo") != "Não"]

        nomes_ativos = [str(t.get("Nome", "")).upper() for t in tarefas_ativas]
        if 'MÓDULO 02' not in nomes_ativos:
            return []

        ids_invalidos = []
        for t in tarefas_ativas:
            num_estrutura = t.get("Número_da_estrutura_de_tópicos", "")
            nivel = t.get("Nível_da_estrutura_de_tópicos")
            if num_estrutura.startswith('1.1.1.') and nivel == 6:
                if t.get("MÓDULO_ASC") is None:
                    ids_invalidos.append(t["Id"])
        return ids_invalidos
    
    def verificar_preenchimento(self, tasks: list[dict]) -> list:
        """
        Verifica a falta de preenchimento em campos SAP e de ID Bloco em níveis
        inapropriados da estrutura de tópicos.

        O Conferidor de Projetos verifica a ausência de preenchimento em diversas
        colunas onde elas deveriam estar presentes. As verificações incluem:
        - ID Bloco: em tarefas a partir do nível 3.
        - SAP Elemento PEP: em tarefas a partir do nível 3.
        - SAP Diagrama de Rede: em tarefas a partir do nível 6.
        - SAP Tarefa: em tarefas a partir do nível 5.

        Args:
            tasks (list[dict]): A lista de tarefas a ser verificada.

        Returns:
            list: Uma lista de listas, onde cada lista interna contém o nome da categoria
            (e.g., "ID Bloco") e uma lista de IDs das tarefas com falhas de preenchimento.
        """
        agrupamentos_excluidos = ["pré projeto", "habite-se", "mão de obra rateio", "pré projeto módulo"]
        
        ids_id_bloco = [
            t["Id"] for t in tasks
            if t.get("Nível_da_estrutura_de_tópicos", 0) >= 3
            and t.get("ID_Bloco") is None
            and str(t.get("Agrupamento")).lower() not in agrupamentos_excluidos
        ]
        ids_pep = [
            t["Id"] for t in tasks
            if t.get("Nível_da_estrutura_de_tópicos", 0) >= 3
            and t.get("SAP_Elemento_PEP") is None
            and str(t.get("Agrupamento")).lower() not in ["pré projeto", "habite-se", "mão de obra rateio"]
        ]
        ids_diagrama_rede = [
            t["Id"] for t in tasks
            if t.get("Nível_da_estrutura_de_tópicos", 0) >= 6
            and t.get("SAP_Diagrama_de_Rede") is None
        ]
        ids_tarefa = [
            t["Id"] for t in tasks
            if t.get("Nível_da_estrutura_de_tópicos", 0) >= 5
            and t.get("SAP_Tarefa") is None
            and str(t.get("Agrupamento")).lower() not in ["pré projeto", "habite-se", "mão de obra rateio"]
        ]
        
        return [
            ["ID Bloco", ids_id_bloco],
            ["SAP Elemento PEP", ids_pep],
            ["SAP Diagrama de Rede", ids_diagrama_rede],
            ["SAP Tarefa", ids_tarefa]
        ]
        