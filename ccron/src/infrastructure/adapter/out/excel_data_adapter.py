from ccron.src.domain.ports.excel_data_adapter_interface import ExcelDataAdapterInterface

import openpyxl

class ExcelDataAdapter(ExcelDataAdapterInterface):
    def __init__(self, file_path, sheet_name="Cronograma"):
        self.file_path = file_path
        self.sheet_name = sheet_name

    def read_data(self) -> list[dict]:
        data = []
        try:
            workbook = openpyxl.load_workbook(self.file_path, data_only=True)
            sheet = workbook[self.sheet_name]
            headers = [cell.value for cell in sheet[1]]
            for row in sheet.iter_rows(min_row=2):
                row_data = {headers[i]: cell.value for i, cell in enumerate(row)}
                data.append(row_data)
        except Exception as e:
            # Em um ambiente real, você logaria esse erro
            print(f"Erro ao ler o arquivo Excel: {e}")
            return []
        return data