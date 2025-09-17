import json
import logging
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# This is the same hardened MoodleClient from the previous DAG.
class MoodleClient:
    def __init__(self, moodle_conn_id: str):
        self.http_hook = HttpHook(method='POST', http_conn_id=moodle_conn_id)
        self.token = self.http_hook.get_connection(moodle_conn_id).password
        self.endpoint = f"{self.http_hook.base_url}/webservice/rest/server.php"
        if not self.token: raise ValueError("Moodle token missing from connection password.")

    def _make_request(self, function_name: str, params: dict = None):
        payload = {'wstoken': self.token, 'moodlewsrestformat': 'json', 'wsfunction': function_name}
        if params: payload.update(params)
        response = self.http_hook.run(endpoint=self.endpoint, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
        response.raise_for_status()
        json_response = response.json()
        if isinstance(json_response, dict) and 'exception' in json_response:
            error_msg = f"Moodle API Error for '{function_name}'. Code: {json_response.get('errorcode')}, Msg: {json_response.get('message')}"
            logging.error(error_msg)
            raise AirflowFailException(error_msg)
        return json_response
    
    def create_users(self, users_data: list):
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items(): params[f'users[{i}][{key}]'] = value
        return self._make_request('core_user_create_users', params)

    def update_users(self, users_data: list):
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items(): params[f'users[{i}][{key}]'] = value
        return self._make_request('core_user_update_users', params)

    def delete_users(self, user_ids: list):
        params = {}
        for i, user_id in enumerate(user_ids): params[f'userids[{i}]'] = user_id
        return self._make_request('core_user_delete_users', params)

@dag(
    dag_id='mysql_to_moodle_user_sync',
    start_date=datetime(2025, 9, 16),
    schedule='@hourly',
    catchup=False,
    doc_md="Syncs users from the clean MySQL table to Moodle."
)
def mysql_to_moodle_user_provisioning_dag():
    @task(retries=2, multiple_outputs=True)
    def get_source_and_target_users():
        """
        Phase 1: Reconnaissance.
        Extracts all users from the MySQL source of truth and the Moodle target.
        """
        # 1. Extract from MySQL Cleaned Table (The Source of Truth)
        logging.info("Connecting to MySQL to get the source of truth...")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        source_df = mysql_hook.get_pandas_df("SELECT uid, first_name, last_name, mail as email FROM AUST.ldap_users_cleaned;")
        source_users = source_df.to_dict('records') # Convert DataFrame to list of dicts

        # 2. Extract from Moodle (The Target)
        logging.info("Connecting to Moodle to get current roster...")
        client = MoodleClient(moodle_conn_id='moodle_api')
        moodle_users = client._make_request('core_user_get_users', {'criteria[0][key]': 'email', 'criteria[0][value]': '%%'})
        
        logging.info(f"Recon Complete: Found {len(source_users)} source users in MySQL and {len(moodle_users.get('users', []))} target users in Moodle.")
        return {'source_users': source_users, 'target_users': moodle_users.get('users', [])}

    @task
    def sync_users(source_users: list, target_users: list):
        """
        Phase 2 & 3: Strategy and Execution.
        Compares user lists and executes create, update, and delete operations.
        """
        client = MoodleClient(moodle_conn_id='moodle_api')
        
        # Fetch the list of protected users from the Airflow Variable
        try:
            protected_users_str = Variable.get("moodle_protected_users", default_var='["admin"]')
            protected_usernames = json.loads(protected_users_str)
            logging.info(f"Protecting usernames from deletion: {protected_usernames}")
        except json.JSONDecodeError:
            logging.error("The 'moodle_protected_users' Variable is not valid JSON. Defaulting to ['admin'].")
            protected_usernames = ['admin']

        # Use email as the unique key for comparison
        source_users_map = {user['email'].lower(): user for user in source_users if user.get('email')}
        target_users_map = {user['email'].lower(): user for user in target_users if user.get('email')}

        # STRATEGY 1: CREATE users who are in the source but not in the target
        users_to_create_data = [user for email, user in source_users_map.items() if email not in target_users_map]
        if users_to_create_data:
            moodle_payload = [
                {'username': u['uid'], 'password': 'PasswordNeedsReset123!', 'firstname': u['first_name'], 'lastname': u['last_name'], 'email': u['email'], 'auth': 'manual'}
                for u in users_to_create_data
            ]
            logging.info(f"EXECUTION: Creating {len(moodle_payload)} new users: {[u['username'] for u in moodle_payload]}")
            client.create_users(moodle_payload)

        # STRATEGY 2: UPDATE users who are in both systems but have different data
        users_to_update_data = []
        for email, source_user in source_users_map.items():
            if email in target_users_map:
                target_user = target_users_map[email]
                if (source_user['first_name'] != target_user.get('firstname', '') or
                    source_user['last_name'] != target_user.get('lastname', '') or
                    source_user['uid'] != target_user.get('username', '')):
                    users_to_update_data.append({
                        'id': target_user['id'],
                        'username': source_user['uid'],
                        'firstname': source_user['first_name'],
                        'lastname': source_user['last_name'],
                        'email': source_user['email']
                    })
        if users_to_update_data:
            logging.info(f"EXECUTION: Updating {len(users_to_update_data)} users: {[u['username'] for u in users_to_update_data]}")
            client.update_users(users_to_update_data)

        # STRATEGY 3: DELETE users who are in the target but not in the source
        user_ids_to_delete = [
            user['id'] for email, user in target_users_map.items()
            if email not in source_users_map and user.get('username') not in protected_usernames
        ]
        if user_ids_to_delete:
            logging.info(f"EXECUTION: Deleting {len(user_ids_to_delete)} users: ID numbers {user_ids_to_delete}")
            client.delete_users(user_ids_to_delete)
        
        logging.info("Sync complete. Moodle is now aligned with the MySQL source of truth.")

    # Define the DAG flow
    user_data = get_source_and_target_users()
    sync_users(source_users=user_data['source_users'], target_users=user_data['target_users'])

mysql_to_moodle_user_provisioning_dag()