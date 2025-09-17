# GigaChatBot's Fortified Airflow Environment (FINAL)

# This base image name comes directly from your docker-compose.yml
FROM apache/airflow:3.0.6

# Switch to root user to install system packages
USER root

# Install the OpenLDAP C development libraries required to build python-ldap
RUN apt-get update && apt-get install -y libldap2-dev libsasl2-dev && apt-get clean

# Switch back to the airflow user
USER airflow

# NOW, ONLY INSTALL THE CORE LIBRARY. THIS WILL SUCCEED.
RUN pip install --no-cache-dir python-ldap