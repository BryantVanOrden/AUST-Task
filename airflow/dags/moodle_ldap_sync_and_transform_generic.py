import json
from datetime import datetime
import ldap
import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# --- MoodleClient (Unchanged) ---
class MoodleClient:
    def __init__(self, moodle_conn_id: str):
        self.http_hook = HttpHook(method='POST', http_conn_id=moodle_conn_id)
        self.token = self.http_hook.get_connection(moodle_conn_id).password
        self.endpoint = f"{self.http_hook.base_url}/webservice/rest/server.php"
        if not self.token:
            raise ValueError("Moodle token is missing from the connection's password field. Discipline!")

    def _make_request(self, function_name: str, params: dict = None):
        payload = {'wstoken': self.token, 'moodlewsrestformat': 'json', 'wsfunction': function_name}
        if params: payload.update(params)
        response = self.http_hook.run(endpoint=self.endpoint, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
        response.raise_for_status()
        json_response = response.json()
        if isinstance(json_response, dict) and 'exception' in json_response:
            raise Exception(f"Moodle API Error for {function_name}: {json_response.get('message')}")
        return json_response

# --- Helper Function for Dynamic Moodle Schema (Unchanged) ---
def generate_create_table_sql_from_df(df: pd.DataFrame, table_name: str, p_keys: list = ['id']) -> tuple[str, str]:
    type_mapping = {'int64': 'BIGINT', 'float64': 'FLOAT', 'bool': 'BOOLEAN', 'object': 'TEXT'}
    cols = []
    for col, dtype in df.dtypes.items():
        mysql_type = type_mapping.get(str(dtype), 'TEXT')
        safe_col = ''.join(c for c in col if c.isalnum() or c == '_')
        cols.append(f"`{safe_col}` {mysql_type}")
    if p_keys:
        p_key_str = ", ".join([f"`{k}`" for k in p_keys])
        cols.append(f"PRIMARY KEY ({p_key_str})")
    drop_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_sql = f"CREATE TABLE {table_name} ({', '.join(cols)});"
    return drop_sql, create_sql


@dag(
    dag_id='moodle_ldap_dynamic_etl_v2',
    start_date=datetime(2025, 9, 16),
    schedule='@daily',
    catchup=False,
    tags=['moodle', 'ldap', 'bi', 'dynamic'],
)
def moodle_ldap_data_sync_dag():
    
    @task
    def extract_ldap_data() -> list:
        print("Connecting to LDAP...")

        conn = BaseHook.get_connection('ldap_default')
        LDAP_URI = f"ldap://{conn.host}:{conn.port}"

        ADMIN_DN = conn.login

        ADMIN_PW = conn.password
        ldap_conn = ldap.initialize(LDAP_URI)
        ldap_conn.simple_bind_s(ADMIN_DN, ADMIN_PW)
        conn_extra = conn.extra_dejson
        search_base = conn_extra.get('search_base', 'dc=example,dc=com')
        search_filter = conn_extra.get('search_filter', '(objectClass=inetOrgPerson)')
        results = ldap_conn.search_s(search_base, ldap.SCOPE_SUBTREE, search_filter, None)
        ldap_conn.unbind_s()
        
        # --- FIX 1: Removed the unnecessary and faulty .decode() calls ---
        # The ldap library for Python 3 returns decoded strings by default.
        users = [{'dn': dn, **{k: [v.decode() for v in val] for k, val in entry.items()}} for dn, entry in results]
        
        print(f"Extracted {len(users)} users from LDAP.")
        return users

    
    @task
    def extract_moodle_data() -> dict:
        client = MoodleClient(moodle_conn_id='moodle_api')
        print("Extracting Moodle data...")

        # Users
        all_users_resp = client._make_request('core_user_get_users', {
            'criteria[0][key]': 'email', 'criteria[0][value]': '%%'
        })
        users = all_users_resp.get('users', [])
        user_ids = [u.get('id') for u in users if isinstance(u, dict) and 'id' in u]

        # Courses
        courses = client._make_request('core_course_get_courses')
        course_ids = [c.get('id') for c in courses if isinstance(c, dict) and 'id' in c]

        # Assignments per course (these DO use 'id')
        assignments = []
        if course_ids:
            params = {f'courseids[{i}]': cid for i, cid in enumerate(course_ids)}
            assigns_resp = client._make_request('mod_assign_get_assignments', params)
            for crs in assigns_resp.get('courses', []):
                assignments.extend(crs.get('assignments', []))
        assign_ids = [a.get('id') for a in assignments if isinstance(a, dict) and 'id' in a]

        # Submissions per assignment
        assign_submissions = []
        if assign_ids:
            params = {f'assignmentids[{i}]': aid for i, aid in enumerate(assign_ids)}
            subs_resp = client._make_request('mod_assign_get_submissions', params)
            # NOTE: top-level here uses 'assignmentid' (NOT 'id')
            for a in subs_resp.get('assignments', []):
                aid = a.get('assignmentid') or a.get('id')  # be safe
                for s in a.get('submissions', []):
                    s['assignmentid'] = aid
                    assign_submissions.append(s)

        # Grades per assignment
        assign_grades = []
        if assign_ids:
            params = {f'assignmentids[{i}]': aid for i, aid in enumerate(assign_ids)}
            grades_resp = client._make_request('mod_assign_get_grades', params)
            # NOTE: top-level here also uses 'assignmentid'
            for a in grades_resp.get('assignments', []):
                aid = a.get('assignmentid') or a.get('id')
                for g in a.get('grades', []):
                    g['assignmentid'] = aid
                    assign_grades.append(g)

        # OPTIONAL: activity completion (can be chatty: users x courses)
        completion = []
        for uid in user_ids:
            for cid in course_ids:
                try:
                    resp = client._make_request('core_completion_get_activities_completion_status', {
                        'courseid': cid, 'userid': uid
                    })
                    for st in resp.get('statuses', []):
                        # keep raw + identifiers you care about
                        completion.append({'courseid': cid, 'userid': uid, **st})
                except Exception as e:
                    print(f"completion fetch failed for user {uid} course {cid}: {e}")

        print(f"Extracted {len(users)} users, {len(courses)} courses, "
            f"{len(assignments)} assignments, {len(assign_submissions)} submissions, "
            f"{len(assign_grades)} grades, {len(completion)} completion rows.")

        return {
            'users': users,
            'courses': courses,
            'assignments': assignments,
            'submissions': assign_submissions,   # submissions have s['id'] (submission id)
            'assign_grades': assign_grades,      # grades items usually DON'T have 'id'
            'completion': completion,
        }



    @task
    def load_data_to_mysql(ldap_users: list, moodle_data: dict):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        print("Loading data into AUST database...")
        mysql_hook.run("CREATE DATABASE IF NOT EXISTS AUST;")

        # --- LDAP staging ---
        mysql_hook.run("""
            CREATE TABLE IF NOT EXISTS AUST.ldap_users (
                dn VARCHAR(255) PRIMARY KEY,
                cn VARCHAR(255),
                sn VARCHAR(255),
                mail VARCHAR(255),
                uid VARCHAR(255),
                raw_attributes JSON,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
        if ldap_users:
            rows = [
                (u['dn'], u.get('cn', [None])[0], u.get('sn', [None])[0],
                u.get('mail', [None])[0], u.get('uid', [None])[0], json.dumps(u))
                for u in ldap_users
            ]
            mysql_hook.insert_rows(
                table='AUST.ldap_users',
                rows=rows,
                target_fields=['dn', 'cn', 'sn', 'mail', 'uid', 'raw_attributes'],
                replace=True
            )
            print(f"Loaded {len(rows)} records into AUST.ldap_users.")

        # --- Create ALL raw tables up front (so inserts never race missing tables) ---
        # Base simple raw tables that store id + raw_data
        for name in ['users', 'courses', 'assignments', 'submissions']:
            mysql_hook.run(f"""
                CREATE TABLE IF NOT EXISTS AUST.moodle_{name}_raw (
                    id BIGINT PRIMARY KEY,
                    raw_data JSON,
                    last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)

        # Special raw tables that need extra FK columns / no natural id
        mysql_hook.run("""
            CREATE TABLE IF NOT EXISTS AUST.moodle_assign_submissions_raw (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                assignmentid BIGINT,
                raw_data JSON,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
        mysql_hook.run("""
            CREATE TABLE IF NOT EXISTS AUST.moodle_assign_grades_raw (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                assignmentid BIGINT,
                raw_data JSON,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
        mysql_hook.run("""
            CREATE TABLE IF NOT EXISTS AUST.moodle_completion_raw (
                courseid BIGINT,
                userid BIGINT,
                cmid BIGINT,
                raw_data JSON,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (courseid, userid, cmid)
            );
        """)

        # --- Load base datasets ONLY (avoid touching assign_* / completion here) ---
        for name in ['users', 'courses', 'assignments', 'submissions']:
            data = moodle_data.get(name) or []
            if not data:
                continue
            table_name = f"AUST.moodle_{name}_raw"
            rows = [(item['id'], json.dumps(item)) for item in data if isinstance(item, dict) and 'id' in item]
            if rows:
                mysql_hook.insert_rows(
                    table=table_name,
                    rows=rows,
                    target_fields=['id', 'raw_data'],
                    replace=True
                )
                print(f"Loaded {len(rows)} raw records into {table_name}.")

        # --- Load special datasets with correct schemas/columns ---
        if moodle_data.get('assign_submissions'):
            rows = [
                (item.get('assignmentid'), json.dumps(item))
                for item in moodle_data['assign_submissions']
            ]
            if rows:
                mysql_hook.insert_rows(
                    table='AUST.moodle_assign_submissions_raw',
                    rows=rows,
                    target_fields=['assignmentid', 'raw_data'],
                    replace=False
                )
                print(f"Loaded {len(rows)} raw records into AUST.moodle_assign_submissions_raw.")

        if moodle_data.get('assign_grades'):
            rows = [
                (item.get('assignmentid'), json.dumps(item))
                for item in moodle_data['assign_grades']
            ]
            if rows:
                mysql_hook.insert_rows(
                    table='AUST.moodle_assign_grades_raw',
                    rows=rows,
                    target_fields=['assignmentid', 'raw_data'],
                    replace=False
                )
                print(f"Loaded {len(rows)} raw records into AUST.moodle_assign_grades_raw.")

        if moodle_data.get('completion'):
            rows = [
                (item['courseid'], item['userid'], item['cmid'], json.dumps(item))
                for item in moodle_data['completion']
                if all(k in item for k in ['courseid', 'userid', 'cmid'])
            ]
            if rows:
                mysql_hook.insert_rows(
                    table='AUST.moodle_completion_raw',
                    rows=rows,
                    target_fields=['courseid', 'userid', 'cmid', 'raw_data'],
                    replace=True
                )
                print(f"Loaded {len(rows)} raw records into AUST.moodle_completion_raw.")

        print("Raw data loading complete.")

        
    @task
    def transform_moodle_data():
        print("--- Starting Moodle Data Transformation ---")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        engine = mysql_hook.get_sqlalchemy_engine()

        for entity in ['users', 'courses', 'assignments', 'submissions','assign_submissions', 'completion']:
            raw_table = f"AUST.moodle_{entity}_raw"
            clean_table = f"AUST.moodle_{entity}_cleaned"

            # Pull JSON payloads into a DataFrame
            df_raw = pd.read_sql(f"SELECT raw_data FROM {raw_table};", engine)
            if df_raw.empty:
                continue

            # Flatten JSON into tabular structure
            df_clean = pd.json_normalize(df_raw['raw_data'].apply(json.loads))
            if df_clean.empty:
                continue

            # Generate DROP + CREATE table SQL
            drop_sql, create_sql = generate_create_table_sql_from_df(df_clean, clean_table)

            # Run table definition statements
            mysql_hook.run(drop_sql)
            mysql_hook.run(create_sql)

            for col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )

            # Push transformed data into the new table
            df_clean.to_sql(
                clean_table.split('.')[1],
                engine,
                schema='AUST',
                if_exists='append',
                index=False
            )
            print(f"Loaded {len(df_clean)} records into {clean_table}.")

        print("--- Moodle Data Transformation Complete ---")


    @task
    def transform_ldap_data():
        """REVERTED: This is the proven LDAP transformation logic."""
        print("--- Starting LDAP Data Transformation ---")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        df = mysql_hook.get_pandas_df("SELECT * FROM AUST.ldap_users;")
        if df.empty: return
        df['parsed_attrs'] = df['raw_attributes'].apply(json.loads)
        df['title'] = df['parsed_attrs'].apply(lambda x: x.get('title', [None])[0])
        df['first_name'] = df['parsed_attrs'].apply(lambda x: x.get('givenName', [None])[0])
        df['last_name'] = df['parsed_attrs'].apply(lambda x: x.get('sn', [None])[0])
        df['telephone'] = df['parsed_attrs'].apply(lambda x: x.get('telephoneNumber', [None])[0])
        df['address'] = df['parsed_attrs'].apply(lambda x: x.get('postalAddress', [None])[0])
        df['organizational_unit'] = df['dn'].str.split(',').str[1].str.replace('ou=', '')
        cleaned_df = df[['dn', 'uid', 'cn', 'first_name', 'last_name', 'mail', 'title', 'organizational_unit', 'telephone', 'address']]
        engine = mysql_hook.get_sqlalchemy_engine()
        cleaned_df.to_sql('ldap_users_cleaned', engine, schema='AUST', if_exists='replace', index=False)
        print(f"--- LDAP Transformation Complete: Wrote {len(cleaned_df)} records. ---")

    # DAG Flow
    ldap_data = extract_ldap_data()
    moodle_data = extract_moodle_data()
    load_task = load_data_to_mysql(ldap_users=ldap_data, moodle_data=moodle_data)
    
    transform_ldap = transform_ldap_data()
    transform_moodle = transform_moodle_data()
    
    load_task >> [transform_ldap, transform_moodle]

moodle_ldap_data_sync_dag()