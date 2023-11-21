import json
import requests
import pandas as pd
from dateutil import tz
from datetime import datetime
from config import API_TOKEN_ACESSORIAS

from api_asana.api import API_Asana



class API_Acessorias:
    def __init__(self, secret_key_acessorias):
        self.secret_key_acessorias = secret_key_acessorias

    def get_identificadores_api_acessorias(self):

        # url = f'https://api.acessorias.com/companies/*'
        url = "https://app.acessorias.com/sysmainsajax.php?rsargs[]=457&rsargs[]=0&rsargs[]=14&rsargs[]=105&rsargs[]=EmpFantasia%2CEmpID&rsargs[]=Desc&rsargs[]=&rsargs[]=0"
        headers = {
            'Authorization': f'Bearer {self.secret_key_acessorias}'
        }
        response = requests.post(url, headers=headers)
        print(response.text)

    def get_identificadores(self, file_dir):
        try:
            data = pd.read_excel(file_dir)
            data = list(data.dropna(subset="CNPJ")["CNPJ"].values)
            return data
        except Exception as e:
            print(f" ### ERROR GET INDETIFICADORES EXCEL clients.xlsx | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERROR GET INDETIFICADORES EXCEL clients.xlsx",
                notes=f"ERROR : {e} \n >> DETAILS: function: get_identificadores"
            )
    
    def check_request_data(self, request, key, identificador):
        try:
            if request is not None:
                data = json.loads(request.content)
                if len(data[key]) == 0:
                    return None
                else:
                    return data
            else:
                return None
        except Exception as e:
            print(f" ### ERROR CHECK DATA REQUEST | ERROR: {e} | IDENTIFICADOR: {identificador}")
            API_Asana().create_new_task(
                name="ERROR PROCESS API DELIVERIES - CHECK DATA REQUEST",
                notes=f"ERROR : {e} \n >> DETAILS: function: check_request_data \n >> REQUEST: {identificador}"
            )
            return None
        
    def convert_string_to_datetime(self, string):
        try:
            if string != "0000-00-00":
                return datetime.strptime(string, '%Y-%m-%d')
            return None
        except Exception as e:
            print(f" ### ERROR CONVERT STRING TO DATETIME | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERROR PROCESS API DELIVERIES - CONVERT STRING TO DATETIME",
                notes=f"ERROR : {e} \n >> DETAILS: function: convert_string_to_datetime"
            )
            return None
    
    def convert_datetime_to_string(self, date):
        try:
            if date != "0000-00-00":
                return datetime.strftime(data, '%Y-%m-%d' )
            return None
        except Exception as e:
            print(f" ### ERROR CONVERT STRING TO DATETIME | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERROR PROCESS API DELIVERIES - CONVERT DATETIME TO STRING",
                notes=f"ERROR : {e} \n >> DETAILS: function: convert_datetime_to_string"
            )
            return None
    
    def replace_caracters(self, string):
        caract = [".", ",", "-", "/", "*", "#"]
        for i in caract:
            string = string.replace(i, "")
        return string

    def get_deliveries_API_Acessorias(self, identificador, page, data_inicial, data_final):

        data_dict = dict()

        try:
            identificador = self.replace_caracters(string=identificador)
            last_request_id = 1
            for i in range(30):
                # URL de chamada padrão padrão
                # url = f'https://api.acessorias.com/deliveries/{identificador}/?DtInitial=2021-01-01&DtFinal=2023-12-31/obligations&Pagina={page}'
                
                url = f'https://api.acessorias.com/deliveries/{identificador}/?DtInitial={data_inicial}&DtFinal={data_final}/obligations&Pagina={i+1}'
                headers = {
                    'Authorization': f'Bearer {self.secret_key_acessorias}'
                }
                response = requests.get(url, headers=headers)
                last_request_id += 1

                print(f">>> REQUEST TO: {url}")
                print(f"""
                    >> API_KEY: {self.secret_key_acessorias}
                    >> STATUS_CODE: {response.status_code}
                """)

                data = self.check_request_data(request=response, key="Entregas", identificador=identificador)

                if data is not None:
                    print(json.dumps(data, indent=4))

                    entregas = data["Entregas"] #["BALANCETE LANÇADO"]
                    razao = data["Razao"]
                    id_razao = data["ID"]

                    print(f" >>>> loop page: {i} | PROCESSING | IDENTIFICADOR: {identificador} | RAZÃO: {razao} ")
                    try:
                        for entrega in entregas:

                            regime = entrega["Nome"]
                            Status = entrega["Status"]
                            
                            if regime == "BALANCETE LANÇADO":
                                EntCompetencia = self.convert_string_to_datetime(entrega["EntCompetencia"])
                                EntDtPrazo = self.convert_string_to_datetime(entrega["EntDtPrazo"])
                                EntDtAtraso = self.convert_string_to_datetime(entrega["EntDtAtraso"])
                                EntDtEntrega = self.convert_string_to_datetime(entrega["EntDtEntrega"])
                                
                                if EntDtEntrega is not None:
                                    TempoUltimaAcao = datetime.now(tz=tz.gettz("Americas/Sao_Paulo")) -  EntDtEntrega
                                    data_dict.update({
                                        "id_razao": id_razao,
                                        "identificador": identificador,
                                        "razao": razao,
                                        "regime": regime,
                                        "entCompetencia": EntCompetencia,
                                        "entDtPrazo": EntDtPrazo,
                                        "entDtAtraso": EntDtAtraso,
                                        "entDtEntrega": EntDtEntrega,
                                        "tempoUltimaAcao": TempoUltimaAcao.days,
                                        "status_lancamento": Status,
                                        "request_number": i+1,
                                        "last_request_id": i+1,
                                    })
                    except Exception as e:
                        API_Asana().create_new_task(
                            name="ERROR PROCESS API DELIVERIES",
                            notes=f"ERROR : {e} \n >> DETAILS: {identificador} | {entrega} | function: get_deliveries_API_Acessorias"
                        )
                        break
                else:
                    break
            data_dict["last_request_id"] = last_request_id
            return data_dict
        except Exception as e:
            print(f" ### ERROR REQUEST DELIVERIES | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERROR PROCESS API DELIVERIES",
                notes=f"ERROR : {e} \n >> DETAILS: {razao} | {identificador} | function: get_deliveries_API_Acessorias"
            )
            return None
        

if __name__ == "__main__":
    dt_inicio = datetime.now()
    API = API_Acessorias(secret_key=API_TOKEN_ACESSORIAS)

    data = API.get_deliveries(identificador="12.435.883/0001-41", page=100)
    print(data)
    dt_fim = datetime.now()
    duracao = dt_fim - dt_inicio
    print(f"""
        --> Início: {dt_inicio}
        --> Fim: {dt_fim}
        --> Duração: {duracao}
    """)


    