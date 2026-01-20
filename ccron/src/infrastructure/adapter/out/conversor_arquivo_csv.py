from ccron.src.domain.ports.conversor_csv_interface import ConversorArquivoCsvInterface

import csv
import pandas as pd
import chardet
from io import BytesIO
from typing import List, Dict

class ConversorArquivoCsv(ConversorArquivoCsvInterface):
    def csv_de_memoria_para_lista_dict(self, conteudo_bytes: bytes) -> List[Dict]:
        def detectar_encoding(conteudo: bytes) -> str:
            try:
                resultado = chardet.detect(conteudo)
                encoding = resultado['encoding']
                return encoding if encoding else 'utf-16'
            except Exception as e:
                print(f"Erro ao detectar encoding: {e}")
        
        try:
            encoding_detectado = detectar_encoding(conteudo_bytes)
            amostra_str = conteudo_bytes[:4096].decode(encoding_detectado)

            sniffer = csv.Sniffer()
            dialeto = sniffer.sniff(amostra_str)
            delimitador_detectado = dialeto.delimiter
            
            df = pd.read_csv(BytesIO(conteudo_bytes), 
                             encoding=encoding_detectado,
                             sep=delimitador_detectado)
            
            df = df.fillna(value='nan')
            return df.to_dict('records')
        
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")

        