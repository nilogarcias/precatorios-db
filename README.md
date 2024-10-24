# precatorios-db
Estudo sobre os FDIC (Fundo de Investimento em Direitos Creditórios) e a participação dos precatórios em suas carteiras de ativos ao longo do tempo.

# FDIC Database

O que é FDIC? FDIC - Fundo de Investimento em Cotas de Fundos de Investimento em Direitos Creditórios

# Steps to Load FDIC Database

## Required Packages

python-dotenv
psycopg2
beautifulsoup4

## FDIC list from the CVM page

Save the following page as `CVM-DadosCadastrais.html` in the `html` folder (load the page after reply the captcha):

[CVM - DADOS CADASTRAIS](https://sistemas.cvm.gov.br/asp/cvmwww/cadastro/CadListPartic.Asp?Fisic_Juridic=&Tipo_Partic=87&Cpfcgc_Partic=&DtReg_Partic=&ContSocio=)

## Load new FDIC to the database

Running the script `load_fdic.py` in the project folder, the database will be updated with the new FDICs.

## Load FDIC Details from new ones

Running the script `load_fdic_details.py` in the project folder, the database will be updated with the new FDICs details.

## Load FDIC Reports not loaded yet

Running the script `load_fdic_reports_id.py` in the project folder, all FDICs reports not loaded to the database will be inserted.

## Load FDIC Reports Details from new ones

Running the script `load_fdic_reports.py` in the project folder, the database will be updated with the new FDICs reports details.