from ccron.src.domain.ports.excel_data_adapter_interface import ExcelDataAdapterInterface
import pyxlsb
from typing import List, Dict

class ExcelDataAdapter(ExcelDataAdapterInterface):
    def read_data(self, file_path: str, sheet_name: str) -> List[Dict]:
        """
        Lê um arquivo de planilha Excel no formato .xlsb e o converte para uma lista de dicionários.
        """
        data = []
        try:
            with pyxlsb.open_workbook(file_path) as wb:
                with wb.get_sheet(sheet_name) as sheet:
                    rows_iter = sheet.rows()
                    headers = [cell.v for cell in next(rows_iter)]
                    for row in rows_iter:
                        # Create a dictionary for the row, handling None values
                        row_data = {headers[i]: (cell.v if cell.v is not None else None) for i, cell in enumerate(row)}
                        data.append(row_data)
        except Exception as e:
            print(f"Error reading XLSB file: {e}")
            return []
        return data