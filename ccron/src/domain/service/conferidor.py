from ccron.src.domain.ports.conferidor_interface import ConferidorInterface
from ccron.src.domain.ports.excel_binary_data_adapter_interface import ExcelDataAdapterInterface
from ccron.src.infrastructure.adapter.out.excel_binary_data_adapter import ExcelDataAdapter

import re
import datetime
from typing import Optional, List, Dict, Tuple
from itertools import combinations

class Conferidor(ConferidorInterface):
    """
    Encapsula um conjunto de ferramentas para conferir e validar a lógica
    de um cronograma de projeto, identificando possíveis problemas como
    sobreposições de tarefas e grandes hiatos (gaps) na execução.
    """
    def __init__(self):
        self.excel_data_adapter: ExcelDataAdapterInterface = ExcelDataAdapter()
        self.dados_eap = self._puxar_eap_list_dict()

    def get_servicos_simultaneos(self, dados: list[dict], gap_threshold: int = 5) -> tuple[list, list]:
        """
        Orquestra a análise completa do cronograma para encontrar sobreposições e gaps.

        A função primeiro aplica um filtro para selecionar apenas as tarefas relevantes
        e depois itera sobre todos os serviços únicos para executar as verificações.

        Args:
            dados: A lista completa de dicionários com os dados do cronograma.
            gap_threshold: O número de dias para considerar um hiato como "longo".
                           O padrão é 5.

        Returns:
            Uma tupla contendo duas listas:
            - A primeira com os pares de IDs de tarefas sobrepostas.
            - A segunda com os detalhes dos gaps encontrados.
        """
        lista_overlap, lista_long_gaps = [], []
        
        # Extrai os serviços únicos do conjunto de dados completo.
        servicos_unicos = set(item.get('Servicos') for item in dados if item.get('Servicos'))

        # Aplica um filtro de negócio para focar a análise apenas nas tarefas críticas.
        dados_filtrados = [
            item for item in dados
            if (
                (item.get("ÉInfra") and item.get("Nível_da_estrutura_de_tópicos") == 6) or
                (item.get("Tipo_Servico") == "SUPRA") or
                (item.get("Tipo_Servico") == "ASC" and item.get("Nível_da_estrutura_de_tópicos") == 7)
            )
        ]

        # Itera sobre cada serviço para realizar as verificações.
        for servico in servicos_unicos:
            overlap = self.encontrar_sobreposicoes(dados_filtrados, servico)
            long_gaps = self.encontrar_gaps(dados_filtrados, servico, gap_threshold)

            # Agrega os resultados, mantendo a lógica de extração original.
            if overlap:
                for item in overlap:
                    lista_overlap.append([item[0], item[1]]) 
                
            if long_gaps:
                lista_long_gaps.extend(long_gaps)
                    
        return lista_overlap, lista_long_gaps
    
    def encontrar_sobreposicoes(self, dados: list[dict], service: str) -> list | bool:
        """
        Identifica e agrupa todas as ocorrências de um serviço que possuem datas sobrepostas.
        (Documentação completa omitida para brevidade, usar a da resposta anterior)
        """
        service_list = [item for item in dados if item.get('Servicos') == service]
        
        if len(service_list) < 2:
            return False
            
        pares = combinations(service_list, 2)
        overlaps_set = set()
        
        for p1, p2 in pares:
            id1, id2 = p1.get('Id'), p2.get('Id')
            try:
                inicio1 = datetime.datetime.strptime(p1.get('Início'), '%d/%m/%Y')
                termino1 = datetime.datetime.strptime(p1.get('Término'), '%d/%m/%Y')
                inicio2 = datetime.datetime.strptime(p2.get('Início'), '%d/%m/%Y')
                termino2 = datetime.datetime.strptime(p2.get('Término'), '%d/%m/%Y')
            except (ValueError, TypeError):
                continue
            
            if (inicio1 < termino2) and (inicio2 < termino1):
                overlaps_set.add(tuple(sorted([id1, id2])))
        
        if overlaps_set:
            total_overlaps = len(overlaps_set)
            return [[o[0], o[1], total_overlaps] for o in overlaps_set]
        
        return False

    def encontrar_gaps(self, dados: list[dict], service: str, gap_threshold: int) -> list | bool:
        """
        Encontra e mede os hiatos (gaps) entre tarefas consecutivas de um mesmo serviço.
        (Documentação completa omitida para brevidade, usar a da resposta anterior)
        """
        service_tasks_raw = [item for item in dados if item.get('Servicos') == service]

        service_tasks_parsed = []
        for task in service_tasks_raw:
            try:
                task['inicio_dt'] = datetime.datetime.strptime(task.get('Início'), '%d/%m/%Y')
                task['termino_dt'] = datetime.datetime.strptime(task.get('Término'), '%d/%m/%Y')
                service_tasks_parsed.append(task)
            except (ValueError, TypeError):
                continue

        service_tasks = sorted(service_tasks_parsed, key=lambda x: x['termino_dt'])

        if len(service_tasks) < 2:
            return False

        long_gaps = []
        total_gap_acumulado = 0

        for i in range(len(service_tasks) - 1):
            current_task = service_tasks[i]
            next_task = service_tasks[i + 1]
            
            gap_days = (next_task['inicio_dt'] - current_task['termino_dt']).days - 1
            gap_days = max(0, gap_days)

            if gap_days > gap_threshold:
                total_gap_acumulado += gap_days
                long_gaps.append([
                    current_task.get('Id'), 
                    next_task.get('Id'), 
                    gap_days,
                    total_gap_acumulado
                ])

        return long_gaps if long_gaps else False
    
    def get_macrofluxo(self, dados_baseline: List[Dict]) -> List[Dict]:
        baseline_processada, mapa_indice_servico = self._preparar_baseline(dados_baseline)
        dados_p1_final = self._extrair_primeiras_ocorrencias(baseline_processada)

        for item in dados_p1_final:
            self._separar_pred_literal(item)

        for item in dados_p1_final:
            l_s = []
            for rr in item.get('indice', []):
                serv = mapa_indice_servico.get(int(rr))
                l_s.append(serv if serv else "Não Encontrado")
            item['predeServ'] = l_s

        dic_erro = {
            "Id": [], "Servicos": [], "PredecessoraEAP": [],
            "PredecessoraBAS": [], "TipoErro": [], "Descrição": []
        }

        for r in dados_p1_final:
            n = r.get('Servicos')
            ep_raw = [item for item in self.dados_eap if item.get('Servicos') == n]
            l_pre_eap = list(set(item.get('Pred') for item in ep_raw if item.get('Pred')))
            l_t_eap = list(set(item.get('Tipo') for item in ep_raw if item.get('Tipo')))
            
            l_tipo_eap, l_off_eap = [], []
            for ll in l_t_eap:
                try:
                    lx = ll.split("+"); l0 = lx[0]; l11 = lx[1]
                    if l11.endswith("d"): l11 = l11[:-1]
                    l1 = int(l11); l_tipo_eap.append(l0); l_off_eap.append(l1)
                except:
                    try:
                        lx = ll.split("-"); l0 = lx[0]; l11 = lx[1]
                        if l11.endswith("d"): l11 = l11[:-1]
                        l1 = int(l11); l_tipo_eap.append(l0); l_off_eap.append(-l1) 
                    except:
                        l_tipo_eap.append(ll); l_off_eap.append(0)

            l_pre_bas = r.get('predeServ', [])
            l_tipo_bas = r.get('tipo', [])
            l_off_bas = r.get('offset', [])

            dic_pred_eap = {pre: (tip, off) for pre, tip, off in zip(l_pre_eap, l_tipo_eap, l_off_eap)}
            dic_pred_bas = {pre: (tip, off) for pre, tip, off in zip(l_pre_bas, l_tipo_bas, l_off_bas)}
            
            t1, t2, tt, teste_red = False, False, False, False
            key2_last = None

            for key, values in dic_pred_eap.items():
                tt = False 
                if key not in dic_pred_bas:
                    tt, t1 = True, True
                    has_unmatched_base_pred = False
                    for key2 in dic_pred_bas:
                        if key2 not in dic_pred_eap:
                            has_unmatched_base_pred = True
                            key2_last = key2
                    
                    if has_unmatched_base_pred:
                        msg, tp = f"Predecessora {key} substituída por {key2_last}.", "Substituição"
                        teste_red = True
                    else:
                        msg, tp = f"Falta a predecessora {key}.", "Falta"
                else:
                    tipo_eap, off_eap = dic_pred_eap[key]
                    tipo_bas, off_bas = dic_pred_bas[key]
                    if tipo_eap != tipo_bas:
                        tt, msg, tp = True, f"Tipo errado na predecessora {key}.", "Tipo Errado"
                    elif off_eap != off_bas:
                        tt, msg, tp = True, f"Offset errado na predecessora {key}.", "Offset errado"
                
                if tt:
                    dic_erro["Id"].append(r.get("index")); dic_erro["Servicos"].append(n)
                    dic_erro["Descrição"].append(msg); dic_erro["TipoErro"].append(tp)
                    dic_erro["PredecessoraEAP"].append(dic_pred_eap); dic_erro["PredecessoraBAS"].append(dic_pred_bas)

            for pre in l_pre_bas:
                if pre != "Não Encontrado" and pre not in l_pre_eap and not teste_red:
                    dic_erro["Id"].append(r.get("index")); dic_erro["Servicos"].append(n)
                    dic_erro["Descrição"].append(f"Predecessora {pre} adicionada ao Serviço {n}")
                    dic_erro["TipoErro"].append("Adição")
                    dic_erro["PredecessoraEAP"].append(dic_pred_eap); dic_erro["PredecessoraBAS"].append(dic_pred_bas)

        erros_lista = []
        chaves = list(dic_erro.keys())
        for i in range(len(dic_erro["Id"])):
            erros_lista.append({chave: dic_erro[chave][i] for chave in chaves})

        return sorted(erros_lista, key=lambda x: x.get('Servicos', ''))
    
    def format_tabela_list_dict(self,
        lista_overlap: List[list], 
        lista_long_gaps: List[list], 
        dados_projeto: List[Dict]
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Formata os resultados de sobreposições e gaps em tabelas detalhadas e resumidas.
        Versão refatorada para operar com listas de dicionários.
        
        Args:
            lista_overlap: Lista de pares de IDs com sobreposição. Ex: [[id1, id2], ...]
            lista_long_gaps: Lista com detalhes dos gaps. Ex: [[id1, id2, gap, total], ...]
            dados_projeto: A lista completa de todas as tarefas (dicionários).

        Returns:
            Uma tupla com quatro listas de dicionários:
            1. detalhes_overlap: Linhas detalhadas das tarefas com sobreposição.
            2. tabela_overlap: Tabela resumida das sobreposições.
            3. tabela_gap: Tabela resumida dos gaps.
            4. detalhes_gap: Linhas detalhadas das tarefas com gaps.
        """
        detalhes_overlap, tabela_overlap = [], []
        detalhes_gap, tabela_gap = [], []

        mapa_ids_projeto = {item['Id']: item for item in dados_projeto}

        if lista_overlap:
            for id1, id2 in lista_overlap:
                task1 = mapa_ids_projeto.get(id1)
                task2 = mapa_ids_projeto.get(id2)

                if task1 and task2:
                    detalhes_overlap.append(task1)
                    detalhes_overlap.append(task2)
                    
                    tabela_overlap.append({
                        "Sobreposição Entre": f"{task1.get('ID_')} e {task2.get('ID_')}",
                        "Servicos": task1.get("Servicos"),
                        "Id_1": task1.get("Id"),
                        "Id_2": task2.get("Id"),
                        "Início_1": task1.get("Início"),
                        "Término_1": task1.get("Término"),
                        "Início_2": task2.get("Início"),
                        "Término_2": task2.get("Término")
                    })

        if lista_long_gaps:
            dados_filtrados = [
                item for item in dados_projeto
                if (
                    (item.get("ÉInfra") and item.get("Nível_da_estrutura_de_tópicos") == 6) or
                    (item.get("Tipo_Servico") == "SUPRA") or
                    (item.get("Tipo_Servico") == "ASC" and item.get("Nível_da_estrutura_de_tópicos") == 7)
                )
            ]
            mapa_ids_filtrado = {item['Id']: item for item in dados_filtrados}

            for id1, id2, gap, total in lista_long_gaps:
                task1 = mapa_ids_filtrado.get(id1)
                task2 = mapa_ids_filtrado.get(id2)

                if task1 and task2:
                    task1_com_gap = {**task1, "gap": gap, "total": total}
                    task2_com_gap = {**task2, "gap": gap, "total": total}
                    detalhes_gap.extend([task1_com_gap, task2_com_gap])
                    
                    tabela_gap.append({
                        "Hiato Entre": f"{task1.get('ID_')} e {task2.get('ID_')}",
                        "Servicos": task1.get("Servicos"),
                        "Id_1": task1.get("Id"),
                        "Id_2": task2.get("Id"),
                        "Início_1": task1.get("Início"),
                        "Término_1": task1.get("Término"),
                        "Início_2": task2.get("Início"),
                        "Término_2": task2.get("Término"),
                        "gap": gap,
                        "total": total
                    })

        return detalhes_overlap, tabela_overlap, tabela_gap, detalhes_gap
    
    def _puxar_eap_list_dict(self) -> List[Dict]:
        file_path = r"ccron\src\infrastructure\bases\EAP Planejamento - V9.xlsb"
        sheet_name = "Template PC + Drywall Torre 8p"
        dados_brutos = self.excel_data_adapter.read_data(file_path, sheet_name)

        dados_processados = []
        dados_com_id = [item for item in dados_brutos if item.get("ID") is not None]
        for item in dados_com_id:
            servicos = Conferidor._separar_pav_literal(item.get("Atividade EAP Planejamento"))
            pred1 = Conferidor._separar_pav_literal(item.get("Descrição #1"))
            pred2 = Conferidor._separar_pav_literal(item.get("Descrição #2"))
            if servicos == pred1 or servicos == pred2: continue
            
            novo_item = item.copy()
            novo_item['Servicos'] = servicos
            novo_item['Pred1'] = pred1
            novo_item['Pred2'] = pred2
            dados_processados.append(novo_item)

        lista_d1 = [{"Servicos": item["Servicos"], "Pred": item["Pred1"], "Tipo": item["Tipo #1"]} for item in dados_processados if item.get("Pred1") is not None and item.get("Tipo #1") is not None]
        lista_d2 = [{"Servicos": item["Servicos"], "Pred": item["Pred2"], "Tipo": item["Tipo #2"]} for item in dados_processados if item.get("Pred2") is not None and item.get("Tipo #2") is not None]
        return lista_d1 + lista_d2

    def _preparar_baseline(self, dados_crus: List[Dict]) -> Tuple[List[Dict], Dict[int, str]]:
        baseline_processada = []
        for i, item in enumerate(dados_crus):
            novo_item = item.copy()
            novo_item['index'] = i + 1 
            novo_item['Servicos'] = Conferidor._separar_pav_literal(item.get('Nome'))
            baseline_processada.append(novo_item)
        mapa_indice_servico = {item['index']: item['Servicos'] for item in baseline_processada}
        return baseline_processada, mapa_indice_servico

    def _extrair_primeiras_ocorrencias(self, dados: List[Dict]) -> List[Dict]:
        nivel_7_tasks = [t for t in dados if t.get('Nível_da_estrutura_de_tópicos') == 7]
        try:
            nivel_7_tasks_sorted = sorted(nivel_7_tasks, key=lambda t: datetime.datetime.strptime(t.get('Início'), '%d/%m/%Y') if t.get('Início') else datetime.datetime.max)
        except (ValueError, TypeError):
            nivel_7_tasks_sorted = nivel_7_tasks
        primeiras_ocorrencias = {}
        for task in nivel_7_tasks_sorted:
            servico = task.get('Servicos')
            if servico not in primeiras_ocorrencias:
                primeiras_ocorrencias[servico] = task
        return list(primeiras_ocorrencias.values())

    def _separar_pred_literal(self, item: dict):
        base = item.get("Predecessoras")
        if base and isinstance(base, str):
            lista_pred_str = [s.strip() for s in base.split(';')]
            indice, tipo, offset = [], [], []
            for b in lista_pred_str:
                match = re.match(r'(?P<indice>\d+)(?P<tipo>[A-Z]+)?(?P<offset>[+-]?\d+)?d?', b, re.IGNORECASE)
                indice.append(int(match.group("indice")) if match else 0)
                tipo.append(match.group("tipo").upper() if match and match.group("tipo") else "TI")
                offset.append(int(match.group("offset")) if match and match.group("offset") else 0)
            item["indice"], item["tipo"], item["offset"] = indice, tipo, offset
        else:
            item["indice"], item["tipo"], item["offset"] = [0], ["TI"], [0]

    @staticmethod
    def _separar_pav_literal(c: Optional[str]) -> str:
        if c is not None and isinstance(c, str):
            try:
                lista = c.split(" - ")
            except: return ""
            llen = len(lista)
            try:
                int(lista[0][-1])
            except (ValueError, IndexError): return c
            if llen > 2:
                rest = " - ".join(lista[1:])
            else:
                try: rest = lista[1]
                except IndexError: rest = lista[0]
            return rest
        return c