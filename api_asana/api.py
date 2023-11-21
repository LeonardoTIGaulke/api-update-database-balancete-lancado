import json
import requests
from config_api_asana import *

class API_Asana:
    def __init__(self, api_token_asana = API_TOKEN_ASANA):
        self.api_token_asana = api_token_asana
    
    def get_info_me(self):
        try:
            url = "https://app.asana.com/api/1.0/users/me"
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": f"Bearer {self.api_token_asana}",
            }
            response = requests.post(
                url=url,
                headers=headers,
            )
            print(f"\n\n >> STATUS CODE GET INFO ME: {response.status_code}")
            data_response = json.loads(response.content)
            return {"code": 200, "msg": "informations successfully obtained", "data": data_response}
        except Exception as e:
            print(f" ### ERROR CREATE NEW TASK ASANA | ERROR: {e}")
            return {"code": 400, "msg": "error get info me"}

    def create_new_task(self, name: str, notes: str, project = PROJECT_ID, workspace= WORKSPACE_ID, assignee_section = ASSIGNEE_SECTION_ID, assignee = ASSIGNEE):
        task = {
            "data": {
                "name": name,
                "notes": notes,
                "projects": project,
                "workspace": workspace,
                "assignee_section": assignee_section,
                "assignee": assignee,
            }
        }
        try:
            url = "https://app.asana.com/api/1.0/tasks"
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": f"Bearer {self.api_token_asana}",
            }
            response = requests.post(
                url=url,
                headers=headers,
                data=json.dumps(task),
            )
            print(f"\n\n >> STATUS CODE CREATE NEW TASK: {response.status_code}")
            data_response = json.loads(response.content)
            print(json.dumps(data_response, indent=4))
            return {"code": 200, "msg": "task created successfully"}
        except Exception as e:
            print(f" ### ERROR CREATE NEW TASK ASANA | ERROR: {e}")
            return {"code": 400, "msg": "error create task"}