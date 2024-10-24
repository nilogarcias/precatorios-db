import threading
from psycopg2 import sql, extensions
from dbconnect import connect_db, get_all_fdic_cnpj_from_db

# Given a CNPJ, extract the FDIC reports ID's from the REST API request
def extract_fdic_reports(cnpj):
    import requests
    import json
    from datetime import datetime

    # Define the URL for the FDIC reports API
    report_start = 0
    reports_counter = 0
    has_more_reports = True
    reports = []

    while has_more_reports:
        # Load the FDIC reports () from the API
        fdic_reports_api = 'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=2&l=200&o[0][dataEntrega]=desc&idCategoriaDocumento=6&idTipoDocumento=40&idEspecieDocumento=0&cnpjFundo=' + cnpj + '&s=' + str(
            report_start)

        # Send a GET request to the API
        response = requests.get(fdic_reports_api, verify=False)

        # Check if the request was successful
        if response.status_code != 200:
            # Throw an exception if the request was not successful
            raise Exception('Failed to fetch FDIC reports for CNPJ: ' + cnpj)

        # Parse the JSON content
        fdic_reports = json.loads(response.content)
        reports_list = fdic_reports['data']

        # Extract the report ID's from the JSON content
        for report in reports_list:
            reports_counter += 1
            report_id = report['id']
            report_category = report['categoriaDocumento']
            report_type = report['tipoDocumento']
            report_ref_date = datetime.strptime(
                report['dataReferencia'], '%m/%Y')
            report_delivery_date = datetime.strptime(
                report['dataEntrega'], '%d/%m/%Y %H:%M')
            report_status = report['status']
            report_desc_status = report['descricaoStatus']
            report_analyzed = report['analisado'] == 'S'
            report_status_doc = report['situacaoDocumento']

            reports.append({'fdic_cnpj': cnpj,
                            'report_id': report_id,
                            'category': report_category,
                            'type': report_type,
                            'reference_date': report_ref_date,
                            'delivery_date': report_delivery_date,
                            'status': report_status,
                            'desc_status': report_desc_status,
                            'analyzed': report_analyzed,
                            'status_doc': report_status_doc})

        # Check if there are more reports to fetch
        total_reports = fdic_reports['recordsTotal']
        if reports_counter >= total_reports:
            has_more_reports = False
        else:
            has_more_reports = True
            report_start += len(reports_list)

    return reports

# Insert the FDIC reports in the database
def insert_reports(conn, fdic_reports):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        for report in fdic_reports:
            # Check if the report already exists in the database
            cursor.execute(
                "SELECT * FROM fdic_report WHERE id = %s", (report['report_id'],))

            # Fetch the first row from the result
            row = cursor.fetchone()

            if row is None:
                # Insert the report in the database
                cursor.execute("INSERT INTO fdic_report (cnpj, id, category, type, ref_date, delivery_date, status, desc_status, analyzed, status_doc) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                    report['fdic_cnpj'], report['report_id'], report['category'], report['type'], report['reference_date'], report['delivery_date'], report['status'], report['desc_status'], report['analyzed'], report['status_doc']))

                # Commit the changes to the database
                conn.commit()

    finally:
        # Close the cursor
        cursor.close()

def process_fdic_report(cnpj):
    try:
        # Extract the FDIC reports for the given CNPJ
        fdic_reports = extract_fdic_reports(cnpj)

        # Insert the FDIC reports in the database
        insert_reports(conn, fdic_reports)

    except Exception as e:
        print(f"Failed to extract FDIC report for CNPJ: {cnpj}")
        print(e)

    finally:
        # Release a permit from the semaphore
        semaphore.release()


# Connect to the database
conn = connect_db()

batch_size = 20
# Create a semaphore with a maximum of batch_size permits
semaphore = threading.Semaphore(batch_size)

# Create a list to store the threads
threads = []

# Get all FDIC CNPJs from the database
cnpjs = get_all_fdic_cnpj_from_db(conn)

# Iterate over the CNPJs in the batch
for cnpj in cnpjs:
    # Acquire a permit from the semaphore
    semaphore.acquire()

    # Create a thread to execute the task
    thread = threading.Thread(target=process_fdic_report, args=(cnpj[0],))

    # Start the thread
    thread.start()

    # Add the thread to the list
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()
