import threading
from psycopg2 import sql, extensions
from dbconnect import connect_db
import concurrent.futures
import requests

# Given a report id, extract the FDIC monthly report
def extract_fdic_report(report_id):
    from bs4 import BeautifulSoup
    import base64
    import re
    import locale

    locale.setlocale(locale.LC_NUMERIC, 'pt_BR.UTF8')
    locale.setlocale(locale.LC_MONETARY, 'pt_BR.UTF8')
    conv = locale.localeconv()
    currency_symbol = conv['currency_symbol']

    fdic_report_api = 'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?cvm=true&id=' + str(report_id)
    response = requests.get(fdic_report_api, stream=True, verify=False)

    response.raise_for_status()
    # Check if the request was successful
    if response.status_code != 200:
        # Throw an exception if the request was not successful
        raise Exception('Failed to fetch FDIC report: ' + report_id)

    html_content = ''
    for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
        html_content += chunk

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(base64.b64decode(html_content), 'html.parser')
    tables = soup.find_all('table')

    # Remove the first table containing the report header
    tables.pop(0)

    # Extract from the asset table the data
    asset_rows = tables.pop(0).find_all('tr')

    report_rows = []

    # Regex to extract the asset name from the cell content
    desc_report_patern = re.compile(r"((\d*\s*-\s*)|([a-z](\.\d*)*\){1}?\s*))(.*)")
    desc_group = 5

    # Iterate over the asset rows and extract only first level of the asset data
    for row in asset_rows:
        cols = row.find_all('td')

        # if the first column has the style like "padding-left:20px", it's a first level asset
        if 'padding-left:20px' == cols[0].get('style'):
            asset = desc_report_patern.match(cols[0].text.strip()).group(desc_group)
            value_str = cols[1].find('span', class_='dado-valores').text
            value = locale.atof(value_str[len(currency_symbol) + 1:])
            report_rows.append({'report_id': report_id, 'type': 'asset', 'category': 'asset', 'name': asset, 'value': value})

    # Extract from the portfolio by segment table the data
    segment_rows = tables.pop(0).find_all('tr')

    # Iterate over the segment rows and extract only first level of the segment data
    category = ''
    for row in segment_rows:
        cols = row.find_all('td')

        # if the first column has the style like "padding-left:20px", it's a first level segment
        if 'padding-left:20px' == cols[0].get('style'):
            category = desc_report_patern.match(cols[0].text.strip()).group(desc_group)
        elif 'padding-left:40px' == cols[0].get('style'):
            segment = desc_report_patern.match(cols[0].text.strip()).group(desc_group)
            value_str = cols[1].find('span', class_='dado-valores').text
            value = value = locale.atof(value_str[len(currency_symbol) + 1:])
            report_rows.append({'report_id': report_id, 'type': 'segment', 'category': category, 'name': segment, 'value': value})

    return report_rows

# Get all reports from the database where the data was not filled
def get_fdic_reports_not_filled_from_db(conn):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Select all reports from the fdic_report table
        cursor.execute("""
SELECT r.id
  FROM fdic_report r
    LEFT OUTER JOIN fdic_report_data rd
	  ON (r.id = rd.id)
 WHERE rd.id IS NULL""")

        # Fetch all rows from the result
        reports = cursor.fetchall()

    finally:
        # Close the cursor
        cursor.close()

    return reports

# Insert the FDIC report data in the database
def insert_report_data(conn, report_data):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO fdic_report_data (id, type, category, name, value)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Insert the report data in the database
        for data in report_data:
            cursor.execute(
                insert_sql, (data['report_id'], data['type'], data['category'], data['name'], data['value'],))
        
        # Commit the changes to the database
        conn.commit()
    finally:
        # Close the cursor
        cursor.close()

def process_fdic_report_data(report_id):
    try:
        # Extract the FDIC report data for the given report id
        fdic_report_data = extract_fdic_report(report_id)

        # Insert the FDIC report data in the database
        insert_report_data(conn, fdic_report_data)

    except Exception as e:
        print(f"Failed to extract FDIC report data for report_id: {report_id}")
        print(e)

    finally:
        # Release the permit back to the semaphore
        semaphore.release()

conn = connect_db()

report_ids = get_fdic_reports_not_filled_from_db(conn)
batch_size = 20
# Create a semaphore with a maximum of batch_size permits
semaphore = threading.Semaphore(batch_size)

# Create a list to store the threads
threads = []

# Iterate over the report ids in the batch
for report_id in report_ids:
    # Acquire a permit from the semaphore
    semaphore.acquire()

    # Create a thread to execute the task
    thread = threading.Thread(target=process_fdic_report_data, args=(report_id[0],))

    # Start the thread
    thread.start()

    # Add the thread to the list
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

