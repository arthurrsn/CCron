from ccron.src.infrastructure.adapter.out.excel_data_adapter import ExcelDataAdapter
from ccron.src.domain.ports.excel_data_adapter_interface import ExcelDataAdapterInterface
from ccron.src.domain.ports.transform_data_interface import TransformDataInterface

import re
from datetime import datetime

class TransformData(TransformDataInterface):
    """
    Encapsula a lógica de ETL (Extract, Transform, Load) para processar dados de
    cronogramas de projetos.

    Esta classe transforma uma lista de dicionários com dados brutos em uma
    estrutura padronizada e enriquecida, pronta para análise. As operações incluem
    limpeza de dados, geração de IDs hierárquicos, classificação de serviços e
    enriquecimento a partir de uma fonte de dados externa (planilha "De-Para").
    """
    def __init__(self):
        """
        Inicializa o serviço de transformação, pré-carregando os dados de mapeamento.

        A planilha "De-Para" é lida uma única vez no momento da instanciação
        para otimizar o desempenho, evitando múltiplas leituras do arquivo.
        """
        self.dePara_adapter: ExcelDataAdapterInterface = ExcelDataAdapter(file_path=r"ccron\src\infrastructure\bases\DexPara4d.xlsx")
        self.dePara4D = self.dePara_adapter.read_data()

    def convert_to_datetime_string(self, date_str: str) -> str | None:
        """
        Converte uma string de data no formato 'dd/mm/YYYY' para o mesmo formato.

        A função serve para validar e padronizar a string de data. Retorna None
        se a conversão falhar devido a formato inválido ou tipo de dado incorreto.

        Args:
            date_str: A string da data a ser convertida.

        Returns:
            A data formatada como string 'dd/mm/YYYY' ou None em caso de erro.
        """
        try:
            data_dt = datetime.strptime(date_str, '%d/%m/%Y')
            return data_dt.strftime('%d/%m/%Y')
        except (ValueError, TypeError):
            return None
    
    def get_modulo(self, num_estrutura: str, modulo_asc: str) -> str | None:
        """
        Extrai e formata o código do módulo (M.XX) de um serviço.

        A extração prioriza a coluna 'MÓDULO_ASC'. Se esta for nula ou vazia,
        tenta extrair o módulo a partir do número da estrutura de tópicos (ex: "1.2.3").

        Args:
            num_estrutura: O número da estrutura de tópicos da atividade.
            modulo_asc: O valor da coluna 'MÓDULO_ASC'.

        Returns:
            O código do módulo formatado ('M.XX') ou None se não for encontrado.
        """
        if modulo_asc is not None and str(modulo_asc).strip() != '':
            modulo_str = re.sub(r'\D', '', str(modulo_asc))
            if modulo_str:
                return f"M.{modulo_str.zfill(2)}"
        
        if isinstance(num_estrutura, str) and "." in num_estrutura:
            try:
                modulo = num_estrutura.split(".")[1]
                return f"M.{modulo.zfill(2)}"
            except IndexError:
                pass
        
        return None
    
    def get_bloco(self, col_nome: str, col_nivel: int) -> str | None:
        """
        Extrai o código do bloco (B.XX) se a atividade estiver no nível hierárquico 3.

        Busca pelo padrão "BLOCO [número]" no nome da atividade. A extração só
        ocorre se a atividade for de nível 3, conforme regra de negócio.

        Args:
            col_nome: O nome da atividade.
            col_nivel: O nível hierárquico da atividade.

        Returns:
            O código do bloco formatado ('B.XX') ou None.
        """
        if col_nivel == 3 and re.search(r'BLOCO (\d+)', col_nome.upper(), re.IGNORECASE):
            bloco = re.search(r'BLOCO (\d+)', col_nome.upper()).group(1)
            return f"B.{bloco.zfill(2)}"
        
        return None

    def get_infra(self, col_nome: str, col_nivel: int) -> bool | None:
        """
        Classifica uma atividade como sendo de infraestrutura ou não.

        A classificação é baseada em palavras-chave ("bloco", "estrutura") e no
        nível hierárquico da atividade.

        Args:
            col_nome: O nome da atividade.
            col_nivel: O nível hierárquico da atividade.

        Returns:
            True se for infraestrutura, False se não for, ou None se a regra
            não se aplicar à linha.
        """
        if col_nivel == 3 and ("bloco" in col_nome.lower()):
            return True
        elif col_nivel == 4 and ("estrutura" in col_nome.lower()):
            return False
        elif col_nivel == 1:
            return False
        
        return None
    
    def get_pavimento(self, col_nome: str) -> str | None:
        """
        Extrai e formata o código do pavimento (P.XX) a partir do nome da atividade.

        Busca pelo padrão "p[número]" (case-insensitive) no nome.

        Args:
            col_nome: O nome da atividade.

        Returns:
            O código do pavimento formatado ('P.XX') ou None.
        """
        match = re.search(r'p(\d+)', col_nome, re.IGNORECASE)
        if match:
            pavimento = match.group(1)
            return f"P.{pavimento.zfill(2)}"
        
        return None

    def get_tipoServico(self, id_bloco: str, id_pavimento: str) -> str:
        """
        Determina o tipo de serviço (ASC, INFRA, SUPRA) com base nos IDs de bloco e pavimento.

        Args:
            id_bloco: O ID do bloco ('B.XX').
            id_pavimento: O ID do pavimento ('P.XX').

        Returns:
            A string que classifica o tipo de serviço.
        """
        if id_pavimento is None and id_bloco is None:
            return "ASC"
        elif id_pavimento is None and id_bloco is not None:
            return "INFRA"
        
        return "SUPRA"

    def get_id_geral(self, id_pav: str, id_bloco: str, id_mod: str) -> str:
        """
        Monta o ID hierárquico completo da atividade.

        A estrutura do ID varia dependendo dos componentes disponíveis (módulo, bloco, pavimento).

        Args:
            id_pav: O ID do pavimento.
            id_bloco: O ID do bloco.
            id_mod: O ID do módulo.

        Returns:
            O ID geral concatenado.
        """
        if id_pav is None and id_bloco is not None:
            return f"{id_mod}-{id_bloco}"
        elif id_bloco is None:
            return f"{id_mod}"
        
        return f"{id_mod}-{id_bloco}-{id_pav}"
    
    def get_id_codificacao(self, id_geral: str, codificacao: str) -> str | None:
        """
        Gera um ID de codificação final combinando o ID geral e o código do serviço.

        Exemplo: Transforma 'M.01-B.01' e 'XX.YY.01.01.001' em 'B01.P01.001'.

        Args:
            id_geral: O ID hierárquico da atividade.
            codificacao: A codificação base vinda do "De-Para".

        Returns:
            O ID de codificação final ou None em caso de erro.
        """
        try:
            partes_id = id_geral.split("-")[1:]
            sufixo_cod = codificacao.split(".")[-1]
            
            replace_p0 = partes_id[0].replace(".", "")
            replace_p1 = partes_id[1].replace(".", "")
            
            return f"{replace_p0}.{replace_p1}.{sufixo_cod}"
        except:
            return None
    
    def extrai_valor(self, valor: str, regex: str) -> float | None:
        """
        Extrai um valor numérico de uma string, tratando diferentes formatos.

        A função lida com separadores de milhar ('.'), decimais (','), e pode
        extrair números de unidades específicas (ex: '10d' -> 10.0) usando regex.

        Args:
            valor: A string a ser processada.
            regex: Expressão regular opcional para extrair um subpadrão numérico.

        Returns:
            O valor extraído como float ou None se a conversão falhar.
        """
        try:
            if isinstance(valor, str):
                valor = valor.replace(".", "").strip().replace(",", ".")
            
            if regex:
                match = re.search(regex, str(valor), re.IGNORECASE)
                return float(match.group(1)) if match else float(valor)
            
            return float(valor) if valor is not None else None
        except (ValueError, AttributeError):
            return None

    def transform_service(self, servico: str) -> str:
        """
        Isola o nome base do serviço, removendo prefixos de localização.

        Retira partes do nome que indicam módulo, bloco ou pavimento (ex: "P01 - Alvenaria")
        para retornar apenas "Alvenaria".

        Args:
            servico: O nome completo do serviço/atividade.

        Returns:
            O nome do serviço normalizado.
        """
        extrai_modulo = re.split(r'\s*-\s*(MODULO|TRECHO|Módulo)\s*\d+', servico, flags=re.IGNORECASE)
        extrai_pav = re.search(r'(p\d+)\s*-\s*(.+)', servico, re.IGNORECASE)
        extrai_bloco = re.search(r'BL\s*\d+\s*-\s*(.*)', servico, re.IGNORECASE)
        
        if extrai_pav:
            return extrai_pav.group(2).strip()
        if extrai_bloco:
            return extrai_bloco.group(1).strip()
        if extrai_modulo:
            return extrai_modulo[0].strip()
        
        return servico

    def _apply_ffill_logic(self, dados: list[dict], column_name: str):
        """
        Aplica a lógica de 'forward fill' em uma lista de dicionários.

        Propaga o último valor válido de uma coluna para as linhas subsequentes
        que possuem valor nulo nessa coluna. Simula o comportamento de
        `pandas.DataFrame.fillna(method='ffill')`.

        Args:
            dados: A lista de dicionários a ser modificada (in-place).
            column_name: O nome da chave (coluna) a ser preenchida.
        """
        last_valid_value = None
        for item in dados:
            if item.get(column_name) is not None:
                last_valid_value = item[column_name]
            else:
                item[column_name] = last_valid_value

    def _merge_de_para(self, item: dict) -> dict:
        """
        Enriquece um item com dados da planilha "De-Para" pré-carregada.

        Realiza uma busca (lookup) pelo nome do serviço e, se encontrar uma
        correspondência, adiciona a 'Codificação' ao dicionário do item.

        Args:
            item: O dicionário da atividade a ser enriquecido.

        Returns:
            O dicionário do item, modificado ou não.
        """
        servico_atual = item.get("Servicos")
        match = next((d for d in self.dePara4D if d.get('Services') == servico_atual), None)
        if match:
            item['Codificação'] = match.get('Codificação')

        return item

    def transformar_dados(self, dados: list[dict]) -> list[dict]:
        """
        Orquestra o pipeline completo de transformação dos dados do cronograma.

        O processo é executado em múltiplos passos para garantir que as dependências
        entre os campos calculados sejam resolvidas corretamente (ex: o ID Geral
        depende do Cod_Bloco, que primeiro precisa ser preenchido com 'ffill').

        Args:
            dados: Uma lista de dicionários representando os dados brutos do cronograma.

        Returns:
            Uma lista de dicionários com os dados transformados e enriquecidos.
        """
        dados_finais = []
        
        # --- PASSO 1: Processamento inicial e extração de dados brutos, linha a linha. ---
        # Nesta etapa, os dados são limpos e os campos primários são extraídos.
        # Informações hierárquicas como 'Cod_Bloco' ainda não são propagadas.
        for item in dados:
            item_processado = item.copy()
            nome = str(item_processado.get("Nome", "")).strip()
            
            if not nome:
                continue
            
            nome_lower = nome.lower()
            
            if nome_lower == "nan" or 'loja' in nome_lower.split():
                continue
            
            item_processado["Nome"] = nome
            item_processado["Predecessoras"] = str(item_processado.get("Predecessoras", "")).strip()

            for col in ["Início", "Término", "Início_real", "Término_real"]:
                item_processado[col] = self.convert_to_datetime_string(item_processado.get(col))
            
            num_estrutura = item_processado.get("Número_da_estrutura_de_tópicos")
            modulo_asc = item_processado.get("MÓDULO_ASC")
            item_processado["ID_Modulo"] = self.get_modulo(num_estrutura, modulo_asc)

            nivel = item_processado.get("Nível_da_estrutura_de_tópicos")
            item_processado["Cod_Bloco"] = self.get_bloco(nome, nivel)
            
            # Regra de negócio: Reseta o Cod_Bloco em linhas de resumo de Módulo
            if "MÓDULO" in nome.upper():
                item_processado['Cod_Bloco'] = None

            item_processado["ID_Pavimento"] = self.get_pavimento(nome)
            item_processado["Servicos"] = self.transform_service(nome)
            item_processado['ÉInfra'] = self.get_infra(nome, nivel)

            if nivel is not None:
                for i in range(1, 8): item_processado[f"Nível_{i}"] = None
                for i in range(int(nivel), 8):
                    item_processado[f"Nível_{i}"] = item_processado.get("Servicos")
            
            item_processado["Duração"] = self.extrai_valor(item_processado.get("Duração"), r'(.+)d|dia')
            item_processado["Trabalho"] = self.extrai_valor(item_processado.get("Trabalho"), r'(.+)h')
            item_processado["Peso"] = self.extrai_valor(item_processado.get("Peso"), None)
            
            dados_finais.append(item_processado)

        # --- PASSO 2: Propagação de dados hierárquicos (ffill). ---
        # Com os dados iniciais extraídos, agora propagamos o contexto (bloco, infra)
        # para as atividades filhas que herdam essas características.
        self._apply_ffill_logic(dados_finais, 'Cod_Bloco')
        self._apply_ffill_logic(dados_finais, 'ÉInfra')
        
        max_niveis = int(max((d.get('Nível_da_estrutura_de_tópicos', 0) or 0 for d in dados_finais), default=0))
        
        for i in range(1, max_niveis + 1):
            self._apply_ffill_logic(dados_finais, f"Nível_{i}")

        # --- PASSO 3: Cálculos finais, enriquecimento e aplicação de regras de negócio. ---
        # Com a estrutura de dados completa e preenchida, realizamos os cálculos
        # que dependem do contexto hierárquico.
        for item in dados_finais:
            id_modulo = item.get("ID_Modulo")
            cod_bloco = item.get("Cod_Bloco")
            id_pavimento = item.get("ID_Pavimento")
            
            # Gera IDs e classificações que dependem dos dados preenchidos no Passo 2
            item["ID_"] = self.get_id_geral(id_pavimento, cod_bloco, id_modulo)
            item["Tipo_Servico"] = self.get_tipoServico(cod_bloco, id_pavimento)

            # Enriquece o item com a codificação externa do "De-Para"
            item = self._merge_de_para(item)

            # Aplica as lógicas finais para gerar o ID de codificação definitivo
            codificacao = item.get("Codificação")
            item["Id_Codificacao"] = self.get_id_codificacao(item.get("ID_"), codificacao)
            
            if item.get("Tipo_Servico") == "ASC":
                item["Id_Codificacao"] = codificacao
            
            # Regra de negócio específica para serviços de infraestrutura
            if (item.get("ÉInfra") is True and 
                item.get("Nível_da_estrutura_de_tópicos") == 6 and
                codificacao is not None and isinstance(codificacao, str) and
                isinstance(cod_bloco, str)):
                item["Id_Codificacao"] = codificacao.replace("XX", cod_bloco.replace(".", ""))

        return dados_finais