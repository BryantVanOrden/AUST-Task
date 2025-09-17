# AUST Moodle & OpenLDAP Integration Suite

## 1. Project Overview

This suite provides a complete data integration and user management solution for a Moodle instance, using OpenLDAP as the central source of truth for user identity. The system is orchestrated by Apache Airflow and is designed to run in a Docker environment, connecting to services hosted on the local machine.

The primary functions are:
* **Data Synchronization:** An Airflow DAG (`aust_data_sync_dag.py`) performs an ETL process, extracting comprehensive data from Moodle (users, courses, assignments, submissions) and loads it into a dedicated `AUST` MySQL database, ready for BI dashboards and analysis.
* **User Provisioning:** A second Airflow DAG (`aust_user_provisioning_dag.py`) synchronizes users from OpenLDAP to Moodle, automatically creating, updating, or deleting Moodle accounts to reflect the state of the LDAP directory.
* **User Management:** A standalone Python Tkinter UI (`ldap_manager.py`) provides a simple, efficient tool for administrators to add new users directly to the OpenLDAP directory.

---
## 2. System Architecture

This project uses a hybrid architecture where the orchestration layer (Airflow) runs within Docker, while the core services run on the host machine.

### Core Services (Host Machine)
* **Moodle:** The Learning Management System (LMS). Must be running locally with Web Services enabled.
* **OpenLDAP:** The directory service and user source of truth. Must be configured to listen on all network interfaces (`0.0.0.0`), not just `localhost`.
* **MySQL:** The data warehouse for BI analytics. Can be running via MySQL Workbench or as a standalone service.

### Orchestration & Management (Docker / Local)
* **Apache Airflow:** Runs inside Docker to schedule and execute the data sync and user provisioning tasks.
* **Docker Desktop:** Required to build and run the custom Airflow environment.
* **Python/Tkinter:** The local Python environment is used to run the `ldap_manager.py` UI.


---
## 3. Prerequisites

Before you begin, ensure the following are installed and configured on your PC:
1.  **Docker Desktop** for Windows.
2.  **Python 3.12+** installed on your host machine.
3.  Your local instances of **Moodle, OpenLDAP, and MySQL** must be running.

---
## 4. Full System Setup

Follow these steps precisely to build and configure the environment from scratch.

### Step 1: Prepare Project Directory
Ensure your project folder has the following structure:
```
/your-project-folder/
|-- dags/
|   |-- aust_data_sync_dag.py
|   |-- aust_user_provisioning_dag.py
|-- docker-compose.yml
|-- Dockerfile
|-- ldap_manager.py
|-- README.md
```
### Step 2: Configure the Airflow Environment
The Airflow environment is custom-built to include the necessary LDAP libraries.

1.  **Create the `Dockerfile`:** This file defines the custom Airflow image.
    ```dockerfile
    FROM apache/airflow:3.0.6 # Or your specific base image version
    USER root
    RUN apt-get update && apt-get install -y libldap2-dev libsasl2-dev && apt-get clean
    USER airflow
    RUN pip install --no-cache-dir python-ldap requests
    ```

2.  **Configure `docker-compose.yml`:** Modify the default Airflow `docker-compose.yml` to use the `Dockerfile`. In the `x-airflow-common:` section, comment out the `image:` line and uncomment the `build: .` line.
    ```yaml
    x-airflow-common:
      &airflow-common
      # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.6}
      build: .
      # ... rest of the file
    ```

### Step 3: Build and Launch Airflow
Execute these commands from your PowerShell terminal in the project directory.

1.  **Build the custom image:** This may take a few minutes the first time.
    ```powershell
    docker-compose build
    ```
2.  **Launch the Airflow services:**
    ```powershell
    docker-compose up -d
    ```

### Step 4: Configure Airflow Connections
This is a critical step. Go to your Airflow UI (usually `http://localhost:8080` but for me 8081 becaue moodle was on 8080), navigate to **Admin -> Connections**, and create the following three connections:

#### Moodle Connection
* **Connection ID:** `moodle_default`
* **Connection Type:** `HTTP`
* **Host:** `http://host.docker.internal`
* **Password:** `YOUR_MOODLE_API_TOKEN`

#### OpenLDAP Connection
* **Connection ID:** `openldap_default`
* **Connection Type:** `Generic`
* **Host:** `host.docker.internal`
* **Login:** `cn=admin,dc=example,dc=com`
* **Password:** `***`
* **Port:** `389`

#### MySQL Connection
* **Connection ID:** `mysql_default`
* **Connection Type:** `MySQL`
* **Host:** `host.docker.internal`
* **Login:** `***`
* **Password:** `***`
* **Port:** `3300`

---
## 5. Component Guide

### Tkinter User Manager (`ldap_manager.py`)
A standalone GUI for adding users to OpenLDAP.
* **Prerequisites:** Install required Python packages on your host machine:
    ```powershell
    pip install python-ldap sv-ttk
    ```
* **To Run:**
    ```powershell
    python ldap_manager.py
    ```

### Airflow DAGs
Once the connections are configured, Airflow will automatically pick up the DAGs. You can enable them and trigger them from the UI.
* **`aust_data_sync`:** Runs daily. Extracts data from Moodle and loads it into the `AUST` database in MySQL.
* **`aust_user_provisioning`:** Runs hourly. Keeps Moodle users synchronized with the OpenLDAP directory.

---
## 6. Troubleshooting

* **`Connection refused` errors in DAGs:** This almost always means a connection's **Host** field is set to `localhost` instead of the required `host.docker.internal`.
* **`pip install python-ldap` fails on Windows:** This is a build error. The solution is to download the pre-compiled `.whl` file from the [Unofficial Windows Binaries for Python](https://www.lfd.uci.edu/~gohlke/pythonlibs/) page and install it directly with `pip install ./path/to/file.whl`.
* **DAGs not updating or showing strange errors:** If a DAG appears to be running old code despite file changes, the Airflow environment may have a corrupted cache. The final solution is the **Total Reset Protocol**:
    ```powershell
    # WARNING: This deletes all Airflow data, including connections.
    docker-compose down -v
    docker-compose build --no-cache
    docker-compose up -d
    # You must re-create all connections in the UI after this.
    ```