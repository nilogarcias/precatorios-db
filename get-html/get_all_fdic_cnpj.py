
def get_all_fdic_cnpj():
    from bs4 import BeautifulSoup

    # load the HTML content from the file
    with open('html\CVM-DadosCadastrais.htm', 'r') as file:
        html_content = file.read()

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