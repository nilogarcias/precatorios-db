import concurrent.futures
import requests

# Given a report id, extract the FDIC report data page
def get_fdic_report(report_id):
    import base64
    import os

    # Check if the report data is already saved
    if os.path.exists(f'html\\fdic-reports-data\\{report_id}.html'):
        return report_id

    # Define the URL for the FDIC report data URL
    fdic_report_api = 'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?cvm=true&id=' + str(report_id)
    response = requests.get(fdic_report_api, stream=True)

    response.raise_for_status()
    # Check if the request was successful
    if response.status_code != 200:
        # Throw an exception if the request was not successful
        raise Exception('Failed to fetch FDIC report: ' + report_id)

    html_content = ''
    for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
        html_content += chunk

    # Decode the HTML content using Base64
    html_content = base64.b64decode(html_content)

    # Save the report to a file
    with open(f'html\\fdic-reports-data\\{report_id}.html', 'wb') as file:
        file.write(html_content)
    
    return report_id

# Function to list all reports id from the folder html/fdic-reports
def get_all_fdic_reports_id():
    import os

    reports_id = []
    for root, dirs, files in os.walk("html\\fdic-reports"):
        for file in files:
            if file.endswith(".json"):
                reports_id.append(file.replace('.json', ''))

    return reports_id

# Get all FDIC CNPJs from the base HTML file
report_ids = get_all_fdic_reports_id()
batch_size = 25

while len(report_ids) > 0:
    # Pop the first n'th CNPJs from the list and extract FDIC details
    report_ids_batch = []
    while len(report_ids_batch) < batch_size and len(report_ids) > 0:
        report_ids_batch.append(report_ids.pop(0))
    
    # Create a thread pool with a maximum of `batch_size` worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        # Submit each URL to the thread pool for execution
        futures = {executor.submit(get_fdic_report, report_id): report_id for report_id in report_ids_batch}

        # Wait for all tasks to complete and collect the results
        for future in concurrent.futures.as_completed(futures):
            report_id = futures[future]
            try:
                print("Extracted report data for id: " + report_id)
            except Exception as e:
                print(f"Failed to extract FDIC details for CNPJ: {report_id[0]}")
                print(e)
