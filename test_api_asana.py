from config_api_asana import API_TOKEN_ASANA
from api_asana.api import API_Asana

name =  "Teste POST - Leonardo API Asana - #03"
notes =  "Teste descrição POST API - #03"

API = API_Asana()
API.create_new_task(
    name=name,
    notes=notes,
)

