
import requests
import json
import logging
from datetime import datetime
from uuid import uuid4

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.variable import Variable
from airflow.providers.http.hooks.http import HttpHook

# =================================================================
# PRE-FLIGHT CHECK: SET THIS UP IN THE AIRFLOW UI. NO EXCUSES.
# =================================================================
#
# 1.  CREATE AN AIRFLOW CONNECTION:
#     - Conn ID:   `moodle_api`
#     - Conn Type: `HTTP`
#     - Host:      `http://host.docker.internal` (or your Moodle URL from within Docker)
#     - Password:  YOUR_MOODLE_API_TOKEN (Place your token here for security)
#
# 2.  (OPTIONAL) CREATE AN AIRFLOW VARIABLE:
#     - Key:       `moodle_test_user_prefix`
#     - Value:     `testwarrior`
#     - This is used to name the temporary user created by the DAG.
#
# =================================================================

class MoodleClient:
    """
    This is your weapon. It executes commands with precision.
    Each method is a different combat technique.
    """
    def __init__(self, moodle_conn_id: str):
        """Initializes the client using an Airflow HTTP Connection."""
        self.http_hook = HttpHook(method='POST', http_conn_id=moodle_conn_id)
        # The token is securely fetched from the connection's password field.
        self.token = self.http_hook.get_connection(moodle_conn_id).password
        if not self.token:
            raise ValueError("Moodle token is missing from the connection password field. Discipline!")

    def _make_request(self, function_name: str, params: dict = None):
        """THE CORE ENGINE. This is where the power is."""
        payload = {
            'wstoken': self.token,
            'moodlewsrestformat': 'json',
            'wsfunction': function_name
        }
        if params:
            payload.update(params)

        logging.info(f"Executing Moodle API strike: {function_name}...")
        try:
            response = self.http_hook.run(
                endpoint='/webservice/rest/server.php',
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            response.raise_for_status() # Check for HTTP errors like 4xx/5xx

            json_response = response.json()
            if isinstance(json_response, dict) and 'exception' in json_response:
                error_msg = (
                    f"Moodle API returned an error for function '{function_name}'. "
                    f"Error Code: {json_response.get('errorcode')}, "
                    f"Message: {json_response.get('message')}"
                )
                logging.error(error_msg)
                raise AirflowFailException(error_msg)

            logging.info(f"--- ✓ SUCCESS: {function_name} ---")
            return json_response
        except requests.exceptions.RequestException as e:
            logging.error(f"---! HTTP REQUEST FAILED for {function_name}: {e} !---")
            raise e


    def create_users(self, users_data: list):
        """CREATE: Forge new warriors."""
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items():
                params[f'users[{i}][{key}]'] = value
        return self._make_request('core_user_create_users', params)

    def update_users(self, users_data: list):
        """UPDATE: Reforge existing warriors."""
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items():
                params[f'users[{i}][{key}]'] = value
        return self._make_request('core_user_update_users', params)

    def delete_users(self, user_ids: list):
        """DELETE: Banish warriors from the roster."""
        params = {}
        for i, user_id in enumerate(user_ids):
            params[f'userids[{i}]'] = user_id
        return self._make_request('core_user_delete_users', params)

    def get_users_by_field(self, field: str, values: list):
        """SEARCH: Find your warriors."""
        params = {'field': field}
        for i, value in enumerate(values):
            params[f'values[{i}]'] = value
        return self._make_request('core_user_get_users_by_field', params)


@dag(
    dag_id='moodle_api_full_health_check',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['moodle', 'healthcheck'],
    doc_md="""
    ### Moodle API Full Health Check

    This DAG performs a full Create, Read, Update, Delete, and Verify (CRUD-V) cycle
    against the Moodle API to ensure all core user functions are operational.
    It uses a secure Airflow Connection (`moodle_api`) to manage credentials.
    """
)
def moodle_api_health_check_dag():

    MOODLE_CONN_ID = "moodle_api"

    @task
    def create_test_user() -> int:
        """CREATE: Forge a new warrior for the test."""
        client = MoodleClient(MOODLE_CONN_ID)
        user_prefix = Variable.get("moodle_test_user_prefix", default_var="testwarrior")
        unique_id = uuid4().hex[:8] # A unique ID to prevent collisions
        username = f"{user_prefix}_{unique_id}"
        email = f"{username}@example.com"

        logging.info(f"Creating test user: {username}")
        new_user_data = [{
            'username': username,
            'password': 'Password123!',
            'firstname': 'HealthCheck',
            'lastname': 'User',
            'email': email,
            'auth': 'manual'
        }]

        created_user_info = client.create_users(new_user_data)
        if not created_user_info or 'id' not in created_user_info[0]:
            raise AirflowFailException("User creation failed or did not return an ID.")

        user_id = created_user_info[0]['id']
        logging.info(f"User created successfully with ID: {user_id}")
        return user_id

    @task
    def update_the_user(user_id: int) -> int:
        """UPDATE: Change the warrior's callsign."""
        client = MoodleClient(MOODLE_CONN_ID)
        updated_lastname = f"User_{uuid4().hex[:8]}"
        logging.info(f"Updating user ID {user_id} with new lastname: {updated_lastname}")
        user_to_update = [{'id': user_id, 'lastname': updated_lastname}]
        client.update_users(user_to_update)
        return user_id, updated_lastname

    @task
    def verify_user_update(user_info: tuple):
        """VERIFY: Confirm the warrior's new identity."""
        user_id, expected_lastname = user_info
        client = MoodleClient(MOODLE_CONN_ID)
        logging.info(f"Verifying update for user ID: {user_id}")
        user_details = client.get_users_by_field(field='id', values=[user_id])

        if not user_details or 'users' not in user_details or not user_details['users']:
             raise AirflowFailException(f"Verification failed: Could not find user with ID {user_id}.")

        actual_lastname = user_details['users'][0]['lastname']
        if actual_lastname != expected_lastname:
            raise AirflowFailException(
                f"Verification failed! Expected lastname '{expected_lastname}' but got '{actual_lastname}'."
            )
        logging.info("User update verified successfully.")
        return user_id

    @task(trigger_rule='all_done') # This cleanup task must always run
    def delete_the_user(user_id: int) -> int:
        """DELETE: Banish the test warrior. This is a cleanup step."""
        client = MoodleClient(MOODLE_CONN_ID)
        logging.info(f"Deleting user ID: {user_id}")
        client.delete_users([user_id])
        return user_id

    @task
    def verify_user_deletion(user_id: int):
        """CONFIRM: Ensure the warrior has been banished."""
        client = MoodleClient(MOODLE_CONN_ID)
        logging.info(f"Confirming deletion for user ID: {user_id}")
        user_check = client.get_users_by_field(field='id', values=[user_id])
        if user_check['users']:
            raise AirflowFailException(f"Deletion confirmation failed! User ID {user_id} still exists.")
        logging.info("User deletion confirmed. The arena is clean.")

    # --- DEFINE THE COMBAT SEQUENCE ---
    created_user_id = create_test_user()
    user_info_tuple = update_the_user(created_user_id)
    verified_user_id = verify_user_update(user_info_tuple)
    deleted_user_id = delete_the_user(verified_user_id)
    verify_user_deletion(deleted_user_id)

moodle_api_health_check_dag()