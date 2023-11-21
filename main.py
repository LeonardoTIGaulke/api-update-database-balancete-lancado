import json
import pandas as pd
from datetime import datetime

from config import API_TOKEN_ACESSORIAS, USERNAME, PASSWORD, HOST, DATABASE
from api.controllers.controller_api_acessorias import API_Acessorias
from api.db.database import DatabaseProcess


class RunAPI(API_Acessorias):
    def __init__(self, secret_key_acessorias):
        super().__init__(secret_key_acessorias)

    def get_deliveries(self, identificador, data_inicial, data_final):
        data = self.get_deliveries_API_Acessorias(identificador=identificador, data_inicial=data_inicial, data_final=data_final, page=1)
        return data
    
    def query_all_database(self, username, password, database, host):
        conn = DatabaseProcess(
            username=username,
            password=password,
            database=database,
            host=host,
        )
        query = conn.query_all_data(table_name="balancete_lancado")
        return query


if __name__ == "__main__":

    API = RunAPI(secret_key_acessorias=API_TOKEN_ACESSORIAS)
    
    # file_dir = r"I:\\1. Gaulke Contábil\\Administrativo\\9. TI\\2. Base Temp\\0. Acessorias\\clients.xlsx"
    # identificadores = API.get_identificadores(file_dir=file_dir)


    identificadores = DatabaseProcess(username=USERNAME,password=PASSWORD,database=DATABASE,host=HOST).query_all_data_companies(table_name="all_companies")
    print(identificadores)

    list_tempo_api = [
        [], # 0 - inicio
        [], # 1 - fim
        [], # 2 - duracao
        [], # 3 - identificador
        [], # 4 - razao
        [], # 5 - tt requests
    ]

    for identificador in identificadores:
        dt_inicio = datetime.now()
        print(f">>>>> identificador: {identificador}")

        data = API.get_deliveries(identificador=identificador, data_inicial="2023-01-01", data_final="2023-12-31")
        print("\n\n ---------> DATA")
        print(data)
        
        if data is not None:
            if data.get("id_razao"):
                DatabaseProcess(username=USERNAME,password=PASSWORD,database=DATABASE,host=HOST).update_regime_lancamento(data=data, table_name="balancete_lancado")

        dt_fim = datetime.now()
        duracao = dt_fim - dt_inicio

        list_tempo_api[0].append(dt_inicio)
        list_tempo_api[1].append(dt_fim)
        list_tempo_api[2].append(duracao)
        list_tempo_api[3].append(identificador)
        
        try:
            list_tempo_api[4].append(data["razao"])
        except:
            list_tempo_api[4].append(None)
        # ---
        try:
            list_tempo_api[5].append(data["request_number"])
        except:
            list_tempo_api[5].append(None)


        print(f"""
            --> identificador: {identificador}
            --> Início: {dt_inicio}
            --> Fim: {dt_fim}
            --> Duração: {duracao}
        """)
    
    try:
        df = pd.DataFrame(
            list(
                zip(
                    list_tempo_api[0], list_tempo_api[1], list_tempo_api[2], list_tempo_api[3], list_tempo_api[4], list_tempo_api[5],
                )
            ),
            columns = [ "inicio", "fim", "duracao", "identificador", "razao", "tt requests" ]
        )
        print(df)
        # df.to_excel("tempo_operacoes_api.xlsx")
    except:
        pass
                
