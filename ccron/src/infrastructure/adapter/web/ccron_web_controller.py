from fastapi import FastAPI, UploadFile, File, HTTPException, logger 
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional

import traceback
from ccron.src.domain.ports.conversor_csv_interface import ConversorArquivoCsvInterface
from ccron.src.infrastructure.adapter.out.conversor_arquivo_csv import ConversorArquivoCsv
from ccron.src.application.service.analise_service import AnaliseService
from ccron.src.domain.ports.analise_service_interface import AnaliseServiceInterface
from ccron.src.infrastructure.adapter.out.integracao_project_adapter import IntegracaoProjectAdapter
from ccron.src.domain.ports.integracao_project_adapter import IntegracaoProjectAdapterInterface

analise_service: AnaliseServiceInterface = AnaliseService()
conversor: ConversorArquivoCsvInterface = ConversorArquivoCsv()
project_dados: IntegracaoProjectAdapterInterface = IntegracaoProjectAdapter()

app = FastAPI(
    title="API Conferidor de Cronogramas",
    description="Processa, valida e fornece dados para análise de cronogramas.",
    version="2.0.0",
)

@app.post("/ccron/analise/completa", tags=["Análise"])
async def analisar_cronograma(file: UploadFile = File(..., description="Relatório exportado do MS Project.")):
    """
    Rota principal: recebe um cronograma, executa a análise e validação,
    e retorna um JSON com todos os resultados.
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Formato de arquivo inválido. Apenas .csv é aceito.")
    try:
        conteudo_bytes = await file.read()
        dados_brutos = conversor.csv_de_memoria_para_lista_dict(conteudo_bytes)
        if not dados_brutos:
            raise HTTPException(status_code=400, detail="Arquivo CSV vazio ou mal formatado.")

        resultado_final = analise_service.analisar_cronograma(dados_brutos)
        conteudo_serializavel = jsonable_encoder(resultado_final)
        return JSONResponse(status_code=200, content=conteudo_serializavel)
    except Exception as e:
        logger.error(f"Erro inesperado na rota /analise/completa: {e}", exc_info=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Erro interno no servidor: {str(e)}")
    

@app.get("/ccron/dados")
async def pegar_tarefas(id: Optional[str] = "82dd99e7-67f6-ee11-8174-00155d806241"):
    dicionario, total = project_dados.pegar_projeto_mrv(id)
    return {
        "total_tarefas": total,
        "tarefas": dicionario  # Retornará no formato {"id": "nome", "id2": "nome2"}
    }

@app.get("/ccron/dump-dados")
async def dump_dados(id: Optional[str] = "82dd99e7-67f6-ee11-8174-00155d806241"):
    dados = project_dados.buscar_dados_brutos_projeto(id)
    return dados

@app.get("/ccron/dados-data")
async def dump_dados(id: Optional[str] = "82dd99e7-67f6-ee11-8174-00155d806241"):
    dados = project_dados.pegar_tarefas_project_data(id)
    return dados