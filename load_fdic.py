from dbconnect import connect_db, get_all_fdic_cnpj_from_db

def extract_fdic_data(html_content):
    from bs4 import BeautifulSoup
    from urllib.parse import urlparse, parse_qs

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

# load the HTML content from the file
with open('html/CVM-DadosCadastrais.htm', mode='r', encoding='utf-8') as file:
    html_content = file.read()

# Extract FDIC data from HTML content
fdic_data = extract_fdic_data(html_content)

try:
    # Connect to the PostgreSQL database
    conn = connect_db()

    # Get all FDIC CNPJs from the database
    cnpj_list = get_all_fdic_cnpj_from_db(conn)

    # Filter out the CNPJs that are already present in the database.
    # The FDIC load from html page are in the format: [{'cnpj': '00000000000191'}, {'cnpj': '00000000000272'}, ...]
    # And the FDIC load from database are in the format: [('00000000000191',), ('00000000000272',), ...]
    fdic_data = [fdic for fdic in fdic_data if (fdic['cnpj'],) not in cnpj_list]

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Insert data into the PostgreSQL table
    insert_sql = "INSERT INTO fdic (cnpj) VALUES (%s)"
    for fdic in fdic_data:
        cursor.execute(insert_sql, (fdic['cnpj'],))

    # Commit the transaction
    conn.commit()
finally:
    # Close the database connection
    conn.close()
