import mysql.connector
from dateutil import tz
from datetime import datetime

from api_asana.api import API_Asana

class DatabaseProcess:
    def __init__(self, username, password, host, database):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
    
    def connection_database(self):
        try:
            config = {
                'user': self.username,
                'password': self.password,
                'host': self.host,
                'database': self.database,
            }

            conn = mysql.connector.connect(**config)
            return conn
        except Exception as e:
            print(f" ### ERROR CONNECTION DATABASE | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERRO CONN DATABASE",
                notes=f"ERROR: {e}"
            )
            return None
        
    def close_connection(self, object):
        try:
            object.close()
            print(f" >> DISCONNECT SUCCESS ")
            return True
        except:
            print(f" >> DISCONNECT ERROR ")
            return False
        
    def query_all_data(self, table_name):
        conn = None
        cursor = None
        try:
            conn = self.connection_database()
            cursor = conn.cursor()
            comannd_query = f"""SELECT * FROM {table_name}"""
            
            query = cursor.execute(comannd_query)
            print(query)
        except Exception as e:
            print(f" ### ERROR QUERY ALL DATABASE | ERROR: {e}")
            API_Asana().create_new_task(
                name="ERROR QUERY INFO. DATABASE",
                notes=f"ERROR: {e} | DATAILS: SELECT * FROM {table_name}"
            )
        
        self.close_connection(object=conn)
        self.close_connection(object=cursor)

    def query_identificador(self, table_name, identificador):
        comand_query = f"""SELECT id,id_razao,identificador FROM {table_name} WHERE identificador="{identificador}" """
        return comand_query
    
    def update_regime_lancamento(self, data, table_name):
        conn = None
        cursor = None
        try:
            conn = self.connection_database()
            cursor = conn.cursor()

            try:
                id_razao = data["id_razao"]
                identificador = data["identificador"]
                razao = data["razao"]
                regime = data["regime"]
                entCompetencia = data["entCompetencia"]
                entDtPrazo = data["entDtPrazo"]
                entDtAtraso = data["entDtAtraso"]
                entDtEntrega = data["entDtEntrega"]
                tempoUltimaAcao = data["tempoUltimaAcao"]
                status_lancamento = data["status_lancamento"]
            except Exception as e:
                API_Asana().create_new_task(
                    name="ERROR GET DATA OBJECT UPDATE",
                    notes=f"ERROR : {e} \n >> DETAILS: function update_regime_lancamento "
                )


            # verifica se o registro já foi inserido ao banco de dados
            comand_query_check = self.query_identificador(table_name=table_name, identificador=identificador)
            cursor.execute(comand_query_check)
            result_query = cursor.fetchall()
            
            print(f" >>> {identificador} | TT Registros: {len(result_query)}")

            datetime_now = datetime.now(tz=tz.gettz("Americas/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

            if len(result_query) >= 1:
                try:
                    comand_update = f"""
                    UPDATE {table_name}
                        SET
                            id_razao = "{id_razao}",
                            razao = "{razao}",
                            regime = "{regime}",
                            entCompetencia = "{entCompetencia}",
                            entDtPrazo = "{entDtPrazo}",
                            entDtAtraso = "{entDtAtraso}",
                            entDtEntrega = "{entDtEntrega}",
                            tempoUltimaAcao = {tempoUltimaAcao},
                            status_lancamento = "{status_lancamento}",
                            atualizado_em = "{datetime_now}"
                        WHERE
                            identificador = "{identificador}" and id >= 0
                    """
                    cursor.execute(comand_update)
                    conn.commit()
                    print(comand_update)

                except Exception as e:
                    print(f" ERROR UPDATE DATABASE | ERROR: {e}")
                    API_Asana().create_new_task(
                        name="ERROR UPDATE DELIVERIES DATABASE",
                        notes=f"ERROR : {e} \n >> DETAILS: {razao} | {identificador}"
                    )


            else:
                try:
                    comannd_query = f"""
                        INSERT
                            INTO {table_name}
                            (id_razao, identificador, razao, regime, entCompetencia, entDtPrazo, entDtAtraso, entDtEntrega, tempoUltimaAcao, status_lancamento)
                            VALUES(
                                "{id_razao}",
                                "{identificador}",
                                "{razao}",
                                "{regime}",
                                "{entCompetencia}",
                                "{entDtPrazo}",
                                "{entDtAtraso}",
                                "{entDtEntrega}",
                                {tempoUltimaAcao},
                                "{status_lancamento}"
                            );
                    """
                    cursor.execute(comannd_query)
                    conn.commit()
                    print(comannd_query)
                except Exception as e:
                    print(f" ERROR INSERT DATABASE | ERROR: {e}")
                    API_Asana().create_new_task(
                        name="ERROR INSERT DELIVERIES DATABASE",
                        notes=f"ERROR : {e} \n >> DETAILS: {razao} | {identificador}"
                    )


        except Exception as e:
            print(f" ### ERROR DATABASE UPDATE/INSERT | ERROR: {e}")
            # API_Asana().create_new_task(
            #     name="ERROR OPRATION DELIVERIES DATABASE",
            #     notes=f"ERROR : {e}"
            # )
        
        self.close_connection(object=conn)
        self.close_connection(object=cursor)


    def query_all_data_companies(self, table_name):
        conn = None
        cursor = None
        try:
            conn = self.connection_database()
            cursor = conn.cursor()

        
            comand_query = f"""SELECT cnpj FROM {table_name} """
            cursor.execute(comand_query)
            result_query = cursor.fetchall()
            
            print(f" >>> TT Registros: {len(result_query)}")

            list_cnpj = list()
            if len(result_query) >= 1:
                
                for data in result_query:
                    list_cnpj.append(data[0])
                    print(">>>> REGISTRO: ", data)
            
            self.close_connection(object=conn)
            self.close_connection(object=cursor)

            return list_cnpj
        
        except Exception as e:
            print(f" ### ERROR DATABASE UPDATE/INSERT | ERROR: {e}")
            # API_Asana().create_new_task(
            #     name="ERROR OPRATION DELIVERIES DATABASE",
            #     notes=f"ERROR : {e}"
            # )
        
            self.close_connection(object=conn)
            self.close_connection(object=cursor)
            return None