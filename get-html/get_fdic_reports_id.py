import concurrent.futures
from get_all_fdic_cnpj import get_all_fdic_cnpj

# Given a CNPJ, extract the FDIC reports ID's from the REST API request
def get_fdic_reports(cnpj):
    import requests
    import json
    from datetime import datetime

    # Define the URL for the FDIC reports API
    report_start = 0
    reports_counter = 0
    has_more_reports = True
    reports = []

    while has_more_reports:
        fdic_reports_api = 'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=2&l=200&o[0][dataEntrega]=desc&idCategoriaDocumento=6&idTipoDocumento=40&idEspecieDocumento=0&cnpjFundo=' + cnpj + '&s=' + str(
            report_start)

        # Send a GET request to the API
        response = requests.get(fdic_reports_api)

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

# Function to store the extracted FDIC reports JSON object to file in the html\fdic-reports\<cnpj>.json folder
def save_reports(reports):
    import json
    import os

    if len(reports) == 0:
        return

    # For each report, save the JSON object to a file
    for report in reports:
        # Check if the report file already exists
        if not os.path.exists(f'html\\fdic-reports\\{report["report_id"]}.json'):
            # Save the report JSON object to a file
            with open(f'html\\fdic-reports\\{report["report_id"]}.json', 'w') as file:
                json.dump(report, file, default=str)
                file.flush()
                file.close()

# Get all FDIC CNPJs from the base HTML file
cnpjs = get_all_fdic_cnpj()
batch_size = 20

while len(cnpjs) > 0:
    # Pop the first n'th CNPJs from the list and extract FDIC details
    cnpjs_batch = []
    while len(cnpjs_batch) < batch_size and len(cnpjs) > 0:
        cnpjs_batch.append(cnpjs.pop(0)['cnpj'])
    
    # Create a thread pool with a maximum of `batch_size` worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        # Submit each URL to the thread pool for execution
        futures = {executor.submit(get_fdic_reports, cnpj): cnpj for cnpj in cnpjs_batch}

        # Wait for all tasks to complete and collect the results
        results = {}
        for future in concurrent.futures.as_completed(futures):
            cnpj = futures[future]
            try:
                results[cnpj] = future.result()
            except Exception as e:
                print(f"Failed to extract FDIC details for CNPJ: {cnpj[0]}")
                print(e)

        for cnpj, fdic_report in results.items():
            print("Extracted reports for CNPJ: " + cnpj[0] + " - " + str(len(fdic_report)) + " reports")
            save_reports(fdic_report)
