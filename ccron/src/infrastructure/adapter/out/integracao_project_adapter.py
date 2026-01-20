from ccron.src.domain.ports.integracao_project_adapter import IntegracaoProjectAdapterInterface
import xml.etree.ElementTree as ET
from typing import Dict, Any
import xmltodict
import requests
import re

class IntegracaoProjectAdapter(IntegracaoProjectAdapterInterface):
    def pegar_projeto_mrv(self, project_id: str):
        url = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectServer/Projects('{project_id}')/Tasks?$select=Id,Name"
    
        fed_auth = "77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U1A+VjE0LDBoLmZ8bWVtYmVyc2hpcHwxMDAzMjAwMjhiMDI4ZTgyQGxpdmUuY29tLDAjLmZ8bWVtYmVyc2hpcHxvYnJhMzYwQG1ydi5jb20uYnIsMTM0MTI2OTY3MzQwMDAwMDAwLDEzMzI0NjU5NTI3MDAwMDAwMCwxMzQxMjc4MzEzNDkzNjIyNTIsMTc5LjEwNy45OC4zNCwyLGI4NDk1OTA3LTRiZGMtNDg3OC1hODI4LWE5MDIxOWNhZDM2ZiwsMDAxMTFlZWEtNWI5Yy1hMDE2LTU3ZDgtYzkyNTIyYzFmMTFhLGNhNDdlY2ExLTgwY2MtYTAwMC1lN2NjLTg4ODk3NGU1NDJiMSxjYTQ3ZWNhMS04MGNjLWEwMDAtZTdjYy04ODg5NzRlNTQyYjEsLDAsMTM0MTI3ODMxMzQ5MjA1OTY2LDEzNDEyOTU1OTM0OTIwNTk2NiwsLGV5SjRiWE5mWTJNaU9pSmJYQ0pEVURGY0lsMGlMQ0o0YlhOZmMzTnRJam9pTVNJc0luQnlaV1psY25KbFpGOTFjMlZ5Ym1GdFpTSTZJbTlpY21Fek5qQkFiWEoyTG1OdmJTNWljaUlzSW5WMGFTSTZJa0pvZVZaMk1IbFRRVlZIVEVSNVJsSmZUR05mUVVFaUxDSmhkWFJvWDNScGJXVWlPaUl4TXpReE1qWTVOamN6TkRBd01EQXdNREFpZlE9PSwyNjUwNDY3NzQzOTk5OTk5OTk5LDEzNDEyNjk2NzM0MDAwMDAwMCwyMjNmNjY3OS1hZWE0LTQwNDUtOTUwNC0yNDgxNzIwNWFhY2MsLCwsLCwxMTUyOTIxNTA0NjA2ODQ2OTc2LCwxOTI5MjQsNFg5ckFYdWNibHBvbERFdERjbDUtNnVrcDFFLCxHQXhVSnNTbTk3WFppMVNlVlE0Uis3SEs2Z01lTDQxK3Z0dGo0YjBXaEVuaWZqbzF1V3BqMmwrU0lQSVUvaTdOVzg1R014SVpqMXpwWTZJbVJHaTlrL1M3d2p3c0FwYU1jZ0pEaEdkTWh1U1A5K003dDdHUGdtUWl0ck1WaVVhZWQrYlJqTHVQbFJnNkhUSVZaWUovYkRYS01ZYm96WlFyN0pmVnhSdWhmdThXdVJIckU1UTY5NS9BNGdjMW00eElDV2gzT01Cd0IxcHlUZWNmdTlHazRGYW02SlZWd1hFeUFHV2xqT2NzbW5tMWN3Njd3eG9wUzIydEVFdU9uNnlMdjhyMTQ4b0VYYW04OTlwNHNOQitTVWlLV0RZSXlWWG5JUS9MUVVZdzFTbGtjNjhFMkwyWjh1ZlV5RHBoV3Y4TjUzZHFaZFpkeHN4STgrQkJKSS9zeHc9PTwvU1A+" 
        rt_fa = "N+h/QRFzy95y9O/72B3MaUC70V3ZtRZ/oez69yY1az8mYjg0OTU5MDctNGJkYy00ODc4LWE4MjgtYTkwMjE5Y2FkMzZmIzEzNDEyNjk2NzM0OTUxODUwMiNjYTQ3ZWNhMS04MGNiLWEwMDAtZTdjYy04YzEyYjRjOWM2NTgjb2JyYTM2MCU0MG1ydi5jb20uYnIjMTkyOTI0I2FiRFJmQzhpOXg0d2JRLTFDNG4yZkt2M0VBayNhYkRSZkM4aTl4NHdiUS0xQzRuMmZLdjNFQWu2syssQSLoj6eD1XyhPFVeJ8Cw91Md8Cv1qBLei+ViglcEjOJgLRe9s/G5Zin0xYzK9GtUAAqKK4vPmWq2HPHBOQs5YMuTsdLLfPEdAHQovvrnISw0nu8CHWU6Fx5g/ZM2ghOTeymRcMYY5zDTEn8zc5sQIF9/lE+/Q/Oo3rXtgsZpkk1Z6JDt2jmUZkPADAZceh3q87iu66eY0nq10LbeB5sP2Xr2mvMwt6quRjTo7N8+xV2pM0NMKvmsAqh1Y/jjUTO65TAuv10WAYX8hTbOz13L3t28ItqwUAYoy20Y75uMSJjJvu1upCZHXfDZ6THrWl993nGKG7jSdDunHYuc0QAAAA=="

        headers = {
            'Accept': 'application/xml',
            'Cookie': f'FedAuth={fed_auth}; rtFa={rt_fa}'
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            xml_data = response.content

            namespaces = {
                'atom': 'http://www.w3.org/2005/Atom',
                'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
            }

            root = ET.fromstring(xml_data)
            entries = root.findall('atom:entry', namespaces)
            
            # Dicionário para armazenar {id: nome}
            tarefas_dict = {}
            total_tarefas = len(entries)

            for entry in entries:
                # 1. Extrair o ID bruto (é uma URL)
                id_bruto = entry.find('atom:id', namespaces).text
                
                # 2. Usar Regex para pegar apenas o que está dentro de Tasks('...')
                # Ex: de ".../Tasks('4f5775bd...')" extrai "4f5775bd..."
                match = re.search(r"Tasks\('([^']+)'\)", id_bruto)
                task_id = match.group(1) if match else id_bruto
                
                # 3. Extrair o Nome
                name_el = entry.find('.//d:Name', namespaces)
                task_name = name_el.text.strip() if name_el is not None and name_el.text else "Sem nome"

                # 4. Adicionar ao dicionário
                tarefas_dict[task_id] = task_name

            return tarefas_dict, total_tarefas

        except Exception as e:
            print(f"Erro: {e}")
            return {}, 0
     
    def _get_common_headers(self):
        # Cookies atualizados conforme seu código
        fed_auth = "77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U1A+VjE0LDBoLmZ8bWVtYmVyc2hpcHwxMDAzMjAwMjhiMDI4ZTgyQGxpdmUuY29tLDAjLmZ8bWVtYmVyc2hpcHxvYnJhMzYwQG1ydi5jb20uYnIsMTM0MTI2OTY3MzQwMDAwMDAwLDEzMzI0NjU5NTI3MDAwMDAwMCwxMzQxMjc4MzEzNDkzNjIyNTIsMTc5LjEwNy45OC4zNCwyLGI4NDk1OTA3LTRiZGMtNDg3OC1hODI4LWE5MDIxOWNhZDM2ZiwsMDAxMTFlZWEtNWI5Yy1hMDE2LTU3ZDgtYzkyNTIyYzFmMTFhLGNhNDdlY2ExLTgwY2MtYTAwMC1lN2NjLTg4ODk3NGU1NDJiMSxjYTQ3ZWNhMS04MGNjLWEwMDAtZTdjYy04ODg5NzRlNTQyYjEsLDAsMTM0MTI3ODMxMzQ5MjA1OTY2LDEzNDEyOTU1OTM0OTIwNTk2NiwsLGV5SjRiWE5mWTJNaU9pSmJYQ0pEVURGY0lsMGlMQ0o0YlhOZmMzTnRJam9pTVNJc0luQnlaV1psY25KbFpGOTFjMlZ5Ym1GdFpTSTZJbTlpY21Fek5qQkFiWEoyTG1OdmJTNWljaUlzSW5WMGFTSTZJa0pvZVZaMk1IbFRRVlZIVEVSNVJsSmZUR05mUVVFaUxDSmhkWFJvWDNScGJXVWlPaUl4TXpReE1qWTVOamN6TkRBd01EQXdNREFpZlE9PSwyNjUwNDY3NzQzOTk5OTk5OTk5LDEzNDEyNjk2NzM0MDAwMDAwMCwyMjNmNjY3OS1hZWE0LTQwNDUtOTUwNC0yNDgxNzIwNWFhY2MsLCwsLCwxMTUyOTIxNTA0NjA2ODQ2OTc2LCwxOTI5MjQsNFg5ckFYdWNibHBvbERFdERjbDUtNnVrcDFFLCxHQXhVSnNTbTk3WFppMVNlVlE0Uis3SEs2Z01lTDQxK3Z0dGo0YjBXaEVuaWZqbzF1V3BqMmwrU0lQSVUvaTdOVzg1R014SVpqMXpwWTZJbVJHaTlrL1M3d2p3c0FwYU1jZ0pEaEdkTWh1U1A5K003dDdHUGdtUWl0ck1WaVVhZWQrYlJqTHVQbFJnNkhUSVZaWUovYkRYS01ZYm96WlFyN0pmVnhSdWhmdThXdVJIckU1UTY5NS9BNGdjMW00eElDV2gzT01Cd0IxcHlUZWNmdTlHazRGYW02SlZWd1hFeUFHV2xqT2NzbW5tMWN3Njd3eG9wUzIydEVFdU9uNnlMdjhyMTQ4b0VYYW04OTlwNHNOQitTVWlLV0RZSXlWWG5JUS9MUVVZdzFTbGtjNjhFMkwyWjh1ZlV5RHBoV3Y4TjUzZHFaZFpkeHN4STgrQkJKSS9zeHc9PTwvU1A+" 
        rt_fa = "N+h/QRFzy95y9O/72B3MaUC70V3ZtRZ/oez69yY1az8mYjg0OTU5MDctNGJkYy00ODc4LWE4MjgtYTkwMjE5Y2FkMzZmIzEzNDEyNjk2NzM0OTUxODUwMiNjYTQ3ZWNhMS04MGNiLWEwMDAtZTdjYy04YzEyYjRjOWM2NTgjb2JyYTM2MCU0MG1ydi5jb20uYnIjMTkyOTI0I2FiRFJmQzhpOXg0d2JRLTFDNG4yZkt2M0VBayNhYkRSZkM4aTl4NHdiUS0xQzRuMmZLdjNFQWu2syssQSLoj6eD1XyhPFVeJ8Cw91Md8Cv1qBLei+ViglcEjOJgLRe9s/G5Zin0xYzK9GtUAAqKK4vPmWq2HPHBOQs5YMuTsdLLfPEdAHQovvrnISw0nu8CHWU6Fx5g/ZM2ghOTeymRcMYY5zDTEn8zc5sQIF9/lE+/Q/Oo3rXtgsZpkk1Z6JDt2jmUZkPADAZceh3q87iu66eY0nq10LbeB5sP2Xr2mvMwt6quRjTo7N8+xV2pM0NMKvmsAqh1Y/jjUTO65TAuv10WAYX8hTbOz13L3t28ItqwUAYoy20Y75uMSJjJvu1upCZHXfDZ6THrWl993nGKG7jSdDunHYuc0QAAAA=="

        return {
            'Cookie': f'FedAuth={fed_auth}; rtFa={rt_fa}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0.0.0'
        }
    
    def pegar_tarefas_project_data(self, project_id: str):
        url = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectData/[en-US]/Tasks?$filter=ProjectId eq guid'{project_id}'"
        
        try:
            response = requests.get(url, headers=self._get_common_headers())
            response.raise_for_status()
            
            dados = xmltodict.parse(response.content)
            entries = dados.get('feed', {}).get('entry', [])
            if isinstance(entries, dict): entries = [entries]

            tarefas_limpas = {}
            for entry in entries:
                propriedades = entry.get('content', {}).get('m:properties', {})
                
                # Pega o ID e o Nome
                raw_id = propriedades.get('d:TaskId')
                raw_nome = propriedades.get('d:TaskName')

                # FUNÇÃO DE LIMPEZA: Se for dicionário (ex: com xml:space), pega só o #text
                def limpar_valor(valor):
                    if isinstance(valor, dict):
                        return valor.get('#text', '')
                    return valor if valor else ""

                t_id = limpar_valor(raw_id)
                t_nome = limpar_valor(raw_nome)
                
                if t_id:
                    tarefas_limpas[t_id] = t_nome

            return tarefas_limpas, len(tarefas_limpas)
        except Exception as e:
            print(f"Erro Project Data: {e}")
            return {}, 0

    def buscar_dados_brutos_projeto(self, project_id: str) -> Dict[str, Any]:
        """Pega o dump bruto de ambas as APIs e converte para JSON"""
        url_server = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectServer/Projects('{project_id}')/Tasks"
        url_data = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectData/[en-US]/Tasks?$filter=ProjectId eq guid'{project_id}'"
        
        resultado = {"server_raw": {}, "data_raw": {}, "erros": []}

        # Request Server
        try:
            headers = self._get_common_headers()
            headers['Accept'] = 'application/xml'
            res = requests.get(url_server, headers=headers)
            resultado["server_raw"] = xmltodict.parse(res.content)
        except Exception as e:
            resultado["erros"].append(f"Server Error: {str(e)}")

        # Request Data
        try:
            # Sem Accept fixo para evitar o erro de 'Unsupported media type'
            res = requests.get(url_data, headers=self._get_common_headers())
            resultado["data_raw"] = xmltodict.parse(res.content)
        except Exception as e:
            resultado["erros"].append(f"Data Error: {str(e)}")

        return resultado

    def buscar_dados_brutos_projeto(self, project_id: str) -> Dict[str, Any]:
        url_server = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectServer/Projects('{project_id}')/Tasks"
        url_data = f"https://mrvengenhariasa.sharepoint.com/sites/planejamento2023/_api/ProjectData/[en-US]/Tasks?$filter=ProjectId eq guid'{project_id}'"
        
        resultado_final = {
            "project_server_json": {},
            "project_data_json": {},
            "erros": []
        }

        # 1. Requisição Project Server (Geralmente aceita XML/Atom)
        try:
            headers_server = self._get_common_headers()
            headers_server['Accept'] = 'application/atom+xml,application/xml' # Especifico para Server
            
            res_server = requests.get(url_server, headers=headers_server)
            res_server.raise_for_status()
            resultado_final["project_server_json"] = xmltodict.parse(res_server.content)
        except Exception as e:
            resultado_final["erros"].append(f"Erro no Project Server: {str(e)}")

        # 2. Requisição Project Data (OData - Tentaremos pegar o XML que ele gera)
        try:
            headers_data = self._get_common_headers()
            # Removemos o Accept fixo para deixar o servidor decidir ou forçamos o padrão OData
            headers_data['Accept'] = 'application/xml' 
            
            res_data = requests.get(url_data, headers=headers_data)
            
            # Se der erro de "Unsupported media type", tentamos sem o header Accept
            if res_data.status_code == 415 or "Unsupported media type" in res_data.text:
                del headers_data['Accept']
                res_data = requests.get(url_data, headers=headers_data)

            res_data.raise_for_status()
            resultado_final["project_data_json"] = xmltodict.parse(res_data.content)
        except Exception as e:
            resultado_final["erros"].append(f"Erro no Project Data: {str(e)}")

        # Retorna o dicionário completo com as duas fontes
        return resultado_final['project_data_json']