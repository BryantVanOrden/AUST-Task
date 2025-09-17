# GigaChatBot's Intelligence Sync DAG (FINAL VERSION)

import json
from datetime import datetime
import ldap
import requests
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook

class MoodleClient:
    def __init__(self, conn_id='moodle_default'):
        self.endpoint = "http://host.docker.internal/webservice/rest/server.php"
        connection = BaseHook.get_connection(conn_id)
        self.token = connection.password

    def _make_request(self, function_name, params=None):
        payload = {'wstoken': self.token, 'moodlewsrestformat': 'json', 'wsfunction': function_name}
        if params: payload.update(params)
        response = requests.post(self.endpoint, data=payload, timeout=30)
        response.raise_for_status()
        json_response = response.json()
        if isinstance(json_response, dict) and 'exception' in json_response:
            raise Exception(f"Moodle API Error for {function_name}: {json_response.get('message')}")
        return json_response

@dag(
    dag_id='aust_data_sync', # New DAG ID
    start_date=datetime(2025, 9, 16),
    schedule='@daily',
    catchup=False,
    doc_md="Syncs Moodle and LDAP data to a MySQL DB for BI dashboards."
)
def aust_data_sync_dag():
    @task
    def extract_ldap_data() -> list:
        ldap_conn_details = BaseHook.get_connection('openldap_default')
        uri = f"ldap://{ldap_conn_details.host}:{ldap_conn_details.port}"
        conn = ldap.initialize(uri)
        conn.simple_bind_s(ldap_conn_details.login, ldap_conn_details.password)
        results = conn.search_s('dc=example,dc=com', ldap.SCOPE_SUBTREE, '(objectClass=inetOrgPerson)', None)
        conn.unbind_s()
        users = [{'dn': dn, **{k: [v.decode() if isinstance(v, bytes) else v for v in val] for k, val in entry.items()}} for dn, entry in results]
        return users

    @task
    def extract_moodle_data() -> dict:
        client = MoodleClient(conn_id='moodle_default')
        all_users = client._make_request('core_user_get_users_by_field', {'field': 'email', 'values[0]': '%'})
        courses = client._make_request('core_course_get_courses')
        course_ids = [course['id'] for course in courses]
        all_assignments, all_submissions = [], []
        if course_ids:
            assignments_response = client._make_request('mod_assign_get_assignments', {f'courseids[{i}]': cid for i, cid in enumerate(course_ids)})
            for course_assignments in assignments_response.get('courses', []): all_assignments.extend(course_assignments.get('assignments', []))
            for assignment in all_assignments:
                submissions = client._make_request('mod_assign_get_submission_status', {'assignid': assignment['id']})
                if submissions.get('lastattempt'):
                    for sub in submissions['lastattempt']['submissions']: sub['assignmentid'] = assignment['id']
                    all_submissions.extend(submissions['lastattempt']['submissions'])
        return {'users': all_users, 'courses': courses, 'assignments': all_assignments, 'submissions': all_submissions}

    @task
    def load_data_to_mysql(moodle_data: dict):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        mysql_hook.run("CREATE DATABASE IF NOT EXISTS AUST;")
        # ... (Create table and insert logic is the same)
        if moodle_data.get('users'):
            mysql_hook.run("CREATE TABLE IF NOT EXISTS AUST.moodle_users (id INT PRIMARY KEY, username VARCHAR(100), fullname VARCHAR(255), email VARCHAR(255), firstaccess BIGINT, lastaccess BIGINT, last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, INDEX(username), INDEX(email));")
            mysql_hook.insert_rows(table='AUST.moodle_users', rows=[(u['id'], u.get('username'), u.get('fullname'), u.get('email'), u.get('firstaccess'), u.get('lastaccess')) for u in moodle_data['users']], target_fields=['id', 'username', 'fullname', 'email', 'firstaccess', 'lastaccess'], replace=True)
    
    # Updated task flow - no need to pass ldap_data to the final task if not used
    # ldap_info = extract_ldap_data() # You can re-enable this if you add an ldap_users table
    moodle_info = extract_moodle_data()
    load_data_to_mysql(moodle_data=moodle_info)

aust_data_sync_dag()