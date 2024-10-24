import concurrent.futures
import requests

def extract_fdic_data(html_content):
    from bs4 import BeautifulSoup

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all the rows containing FDIC data
    fdic_rows = soup.find_all('a', class_='MenuItemP')
    
    # Create a list to store the extracted data
    fdic_list = []
    
    # Extract FDIC names and Cpfcgc_Partic from each row
    for row in fdic_rows:
        # for each row, extract the Cpfcgc_Partic from the href attribute from the a tag
        # to do this, parse the href content using the urllib.parse module
        from urllib.parse import urlparse, parse_qs
        fdic_url = parse_qs(urlparse(row['href']).query)
        
        # and extract the Cpfcgc_Partic request parameter from there
        cnpj = fdic_url['Cpfcgc_Partic'][0]

        # Store the extracted data as a dictionary
        fdic_data = {
            'cnpj': cnpj
        }
        
        # Append the dictionary to the list
        fdic_list.append(fdic_data)
    
    return fdic_list

# Given a CNPJ, extract the FDIC details from the HTML page content
def get_fdic_details(cnpj, timeout=30):
    # Check if the detail file already exists
    import os

    if os.path.exists(f'html\\fdic-details\\{cnpj}.html'):
        return cnpj

    fdic_detail_page = 'https://cvmweb.cvm.gov.br/swb/sistemas/scw/cPublica/CConsolFdo/ResultBuscaDocsFdoFIDC.aspx?Fisic_Juridic=PJ&Tipo_Partic=87&Cpfcgc_Partic=' + cnpj
    response = requests.get(fdic_detail_page, timeout=timeout)

    # Check if the request was successful
    if response.status_code != 200:
        # Throw an exception if the request was not successful
        raise Exception('Failed to fetch FDIC details for CNPJ: ' + cnpj)
    
    # Store the html content to a file in the html\fdic-details\<cnpj>.html folder
    with open(f'html\\fdic-details\\{cnpj}.html', 'w') as file:
        file.write(response.text)
        file.flush()
        file.close()
    
    return cnpj
    

# load the HTML content from the file
with open('html\CVM-DadosCadastrais.htm', 'r') as file:
    html_content = file.read()

# Extract FDIC data from HTML content
fdic_data = extract_fdic_data(html_content)

timeout = 30
batch_size = 20

while len(fdic_data) > 0:
    # Pop the first 10 CNPJs from the list and extract FDIC details
    cnpjs_batch = []
    while len(cnpjs_batch) < batch_size and len(fdic_data) > 0:
        cnpjs_batch.append(fdic_data.pop(0)['cnpj'])
    
    # Create a thread pool with a maximum of 5 worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        # Submit each URL to the thread pool for execution
        futures = {executor.submit(get_fdic_details, cnpj, timeout): cnpj for cnpj in cnpjs_batch}

        # Wait for all tasks to complete and collect the results
        results = {}
        for future in concurrent.futures.as_completed(futures):
            try:
                cnpj = futures[future]
                print(f"Extracted FDIC details for CNPJ: {cnpj}")
            except Exception as e:
                print(f"Failed to extract FDIC details for CNPJ: {cnpj[0]}")
                print(e)
