from psycopg2 import sql, extensions
from dbconnect import connect_db, get_fdic_cnpj_not_filled_from_db
import threading
import requests

# Function to extract data in save mode when the tag is not found
def extract_tag_data(tag, id, soup):
    element = soup.find(tag, id=id)
    if element is not None:
        return element.text
    else:
        return None

def extract_subtag_data(tag, subtag, id, soup):
    element = soup.find(tag, id=id)
    if element is not None:
        subelement = element.find(subtag)
        return subelement.text if subelement is not None else None
    else:
        return None

# Given a CNPJ, extract the FDIC details from the HTML page content
def extract_fdic_details(cnpj, timeout=30):
    from bs4 import BeautifulSoup
    from datetime import datetime

    fdic_detail_page = 'https://cvmweb.cvm.gov.br/swb/sistemas/scw/cPublica/CConsolFdo/ResultBuscaDocsFdoFIDC.aspx?Fisic_Juridic=PJ&Tipo_Partic=87&Cpfcgc_Partic=' + cnpj
    response = requests.get(fdic_detail_page, timeout=timeout)

    # Check if the request was successful
    if response.status_code != 200:
        # Throw an exception if the request was not successful
        raise Exception('Failed to fetch FDIC details for CNPJ: ' + cnpj)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    fdic_name = extract_tag_data(tag='span', id='lbNmDenomSocial', soup=soup)
    fdic_cnpj = extract_tag_data(tag='span', id='lbNrPfPj', soup=soup)
    fdic_admin = extract_tag_data(tag='span', id='lbNmDenomSocialAdm', soup=soup)
    fdic_admin_cnpj = extract_tag_data(tag='span', id='lbNrPfPjAdm', soup=soup)
    fdic_director = extract_tag_data(tag='span', id='lbDirFdo', soup=soup)
    fdic_director_document = extract_tag_data(tag='span', id='lbNrPfPjDirFdo', soup=soup)
    fdic_director_phone = extract_tag_data(tag='span', id='lbTelDirFdo', soup=soup)
    fdic_director_email = extract_subtag_data(tag='span', subtag='a', id='lbEmailDirFdo', soup=soup)
    fdic_director_address = extract_tag_data(tag='span', id='lbEndDirFdo', soup=soup)
    fdic_manager = extract_tag_data(tag='span', id='lbNmGestFdo', soup=soup)
    fdic_manager_cnpj = extract_tag_data(tag='span', id='lbNrPfPjGest', soup=soup)
    fdic_manager_director = extract_tag_data(tag='span', id='lbNmDirGest', soup=soup)
    fdic_manager_dir_document = extract_tag_data(tag='span', id='lbNrPfPjDirGest', soup=soup)
    fdic_manager_dir_phone = extract_tag_data(tag='span', id='lbTelDirGest', soup=soup)
    fdic_manager_dir_email = extract_subtag_data(tag='span', subtag='a', id='lbEmailDirGest', soup=soup)
    fdic_manager_dir_address = extract_tag_data(tag='span', id='lbEndDirGest', soup=soup)
    fdic_dt_start = datetime.strptime(extract_tag_data(tag='span', id='lbDtFunc', soup=soup), '%d/%m/%Y')
    fdic_status = extract_tag_data(tag='span', id='lbSitDesc', soup=soup)
    fdic_site = extract_tag_data(tag='span', id='lbInfAdc3', soup=soup)

    return {
        'name': fdic_name,
        'cnpj': fdic_cnpj,
        'administrator': {
            'name': fdic_admin,
            'cnpj': fdic_admin_cnpj,
            'director': fdic_director,
            'dir_cpf': fdic_director_document,
            'dir_phone': fdic_director_phone,
            'dir_email': fdic_director_email,
            'dir_address': fdic_director_address
        },
        'manager': {
            'name': fdic_manager,
            'cnpj': fdic_manager_cnpj,
            'director': fdic_manager_director,
            'dir_cpf': fdic_manager_dir_document,
            'dir_phone': fdic_manager_dir_phone,
            'dir_email': fdic_manager_dir_email,
            'dir_address': fdic_manager_dir_address
        },
        'start_date': fdic_dt_start,
        'status': fdic_status,
        'site': fdic_site
    }

# Get the FDIC Administrator details from the database or, if not present, insert them
def get_or_insert_fdic_administrator(conn, fdic_admin_details):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Check if the administrator with the given CNPJ is already present in the database
        cursor.execute("SELECT name FROM fdic_admin WHERE cnpj = %s",
                       (fdic_admin_details['cnpj'],))
        admin_name = cursor.fetchone()

        # If the administrator is not present, insert the details into the database
        if admin_name is None:
            insert_sql = "INSERT INTO fdic_admin (cnpj, name) VALUES (%s, %s)"
            cursor.execute(
                insert_sql, (fdic_admin_details['cnpj'], fdic_admin_details['name'],))

        return fdic_admin_details['cnpj']

    finally:
        # Close the cursor
        cursor.close()

# Get the FDIC Director details from the database or, if not present, insert them
def get_or_insert_fdic_director(conn, fdic_dir_details):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Check if the director with the given CPF/CNPJ is already present in the database
        cursor.execute("SELECT name FROM fdic_director WHERE cpf = %s",
                       (fdic_dir_details['dir_cpf'],))
        dir_name = cursor.fetchone()

        # If the director is not present, insert the details into the database
        if dir_name is None and fdic_dir_details['dir_cpf'] is not None:
            insert_sql = "INSERT INTO fdic_director (cpf, name, phone, email, address) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(insert_sql, (fdic_dir_details['dir_cpf'], fdic_dir_details['director'],
                           fdic_dir_details['dir_phone'], fdic_dir_details['dir_email'], fdic_dir_details['dir_address'],))

        return fdic_dir_details['dir_cpf']

    finally:
        # Close the cursor
        cursor.close()

# Update FDIC details in the database
def update_fdic_details_in_db(conn, cnpj, fdic_details):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Get the administrator and director CNPJs from the database
        admin_cnpj = get_or_insert_fdic_administrator(
            conn, fdic_details['administrator'])
        admin_dir_cpf = get_or_insert_fdic_director(conn, fdic_details['administrator'])

        # Get the manager and director CNPJs from the database
        manager_cnpj = get_or_insert_fdic_administrator(
            conn, fdic_details['manager'])
        manager_dir_cpf = get_or_insert_fdic_director(conn, fdic_details['manager'])
        
        # Update the FDIC details in the database
        update_sql = """
            UPDATE fdic
            SET name = %s,
                admin_cnpj = %s,
                admin_dir_cpf = %s,
                manager_cnpj = %s,
                manager_dir_cpf = %s,
                start_date = %s,
                status = %s,
                site = %s
            WHERE cnpj = %s
        """
        cursor.execute(update_sql, (
            fdic_details['name'],
            admin_cnpj,
            admin_dir_cpf,
            manager_cnpj,
            manager_dir_cpf,
            fdic_details['start_date'],
            fdic_details['status'],
            fdic_details['site'],
            cnpj
        ))

        # Commit the transaction
        conn.commit()

    finally:
        # Close the cursor
        cursor.close()

def process_fdic_detail(cnpj):
    try:
        # Extract FDIC details for the given CNPJ
        fdic_details = extract_fdic_details(cnpj, timeout)

        # Update FDIC details in the database
        update_fdic_details_in_db(conn, cnpj, fdic_details)

    except Exception as e:
        print(f"Failed to extract FDIC details for CNPJ: {cnpj}")
        print(e)

    finally:
        # Release a permit from the semaphore
        semaphore.release()

conn = connect_db()
timeout = 30
batch_size = 20
# Create a semaphore with a maximum of batch_size permits
semaphore = threading.Semaphore(batch_size)

# Create a list to store the threads
threads = []

cnpjs = get_fdic_cnpj_not_filled_from_db(conn)

# Iterate over the CNPJs in the batch
for cnpj in cnpjs:
    # Acquire a permit from the semaphore
    semaphore.acquire()

    # Create a thread to execute the task
    thread = threading.Thread(target=process_fdic_detail, args=(cnpj[0],))

    # Start the thread
    thread.start()

    # Add the thread to the list
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()
