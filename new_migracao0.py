import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
import logging
import sys
import re
from utilitarios.backup_limpeza import backup_simples
from utilitarios.validacoes import existe_dir
from sqlalchemy import create_engine
from tqdm import tqdm

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/migracao.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")



# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

# verificando e gerando o backup dos dados. 
database = r'./database/'



all_files_database = list(filter(lambda x: '.db' in x, os.listdir(database)))

if len(all_files_database) >= 1:
    backup_simples(diretorio_origem=database, nome_zipado=f"backup_database_{datazip}.zip", extensao='.db', diretorio_destino=f"{database}backup/")


print(f"Início do processo: {agora.date()} às {agora.time()}")
logging.info(f"Início do processo: {agora.date()} às {agora.time()}")

# Define os diretórios
br_diretorio = r'./br_base/'
#en_diretorio = r'./en_base/'

# Verifica se existe o diretorios
for diretorio in [database, br_diretorio]:
    existe_dir(diretorio)

try :
    sqlite3.connect("./database/br_base_cnpj.db")
    #sqlite3.connect("./database/en_base_cnpj.db")
    engine = create_engine("sqlite:///database/br_base_cnpj.db", connect_args={'sqlite3_max_variables': 10000})
    #engine = create_engine("sqlite:///database/en_base_cnpj.db", connect_args={'sqlite3_max_variables': 10000})

except sqlite3.Error as e:
    print(f"Erro ao conectar com o banco de dados: {e}")
    logging.error(f"Erro ao conectar com o banco de dados: {e}")
    sys.exit(1)

# Cria uma conexão com os SQLites
# with sqlite3.connect("./database/br_base_cnpj.db") as br_conn, sqlite3.connect("./database/en_base_cnpj.db") as en_conn:
with sqlite3.connect("./database/br_base_cnpj.db") as br_conn:
    # Juntar todos os arquivos CSV em um único dataframe para cada país
    br_dados = pd.concat([pd.read_parquet(os.path.join(br_diretorio, f)) for f in os.listdir(br_diretorio) if f.endswith('.gzip')])
    # en_data  = pd.concat([pd.read_parquet(os.path.join(en_diretorio, f)) for f in os.listdir(en_diretorio) if f.endswith('.gzip')])
    br_dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)
    # en_data['TRADING NAME'].replace('--empty--','', regex=True, inplace=True)
    
    """
    # Define a chave primária da tabela
    br_primary_key = ['CNPJ']
    en_primary_key = ['EIN (CNPJ)']

    
    # Salva o dataframe como uma tabela no SQLite e atualiza as linhas existentes
    br_dados.to_sql('estabelecimentos', br_conn, if_exists='append', index=False, method='multi',
                    chunksize=1000, index_label=br_primary_key)
    en_data.to_sql('establishments', en_conn, if_exists='append', index=False, method='multi',
                    chunksize=1000, index_label=en_primary_key)
    """
    contador_geral = 0 
    contador_novos = 0
    contador_inativos = 0
    # Atualiza as colunas "Situação Cadastral" e "Data Situação Cadastral" se as linhas já existem
    #for _, row in tqdm(br_dados.iterrows()):
    for index, row in tqdm(br_dados.iterrows(), total=br_dados.shape[0]):
        contador_geral += 1
        # Verifica se a linha existe
        query = br_conn.execute("SELECT CNPJ, SITUACAO_CADASTRAL, DATA_SITUACAO_CADASTRAL FROM estabelecimentos WHERE CNPJ=?", (row['CNPJ'],))
        result = query.fetchone()

        if result is not None:
            # linha já existe, fazer update
            # Verificando erros
            try:
                if result[1] not in [2, 3, 4]:
                    contador_inativos +=1
                    br_conn.execute(f"""UPDATE estabelecimentos 
                    SET 
                        SITUACAO_CADASTRAL = '{row['SITUACAO_CADASTRAL']}', 
                        DATA_SITUACAO_CADASTRAL = '{row['DATA_SITUACAO_CADASTRAL']}'
                    WHERE CNPJ='{row["CNPJ"]}';""")
            except sqlite3.Error as e:
                print(f"Erro ao executar a query: {e}")
                logging.error(f"Erro ao executar a query: {e}")
                sys.exit(1)
        else:
            # Verificando erros 
            try:
                contador_novos +=1
                # linha não existe, fazer insert
                br_conn.execute(f"""
                INSERT INTO estabelecimentos 
                ('CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
                    'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE1', 'SITE',
                    'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 
                    'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL') 
                VALUES 
                ('{row["CNPJ"]}', '{row["RAZAO_SOCIAL"]}', '{row["NOME_FANTASIA"]}, '{row["RUA"]}',
                '{row["NUMERO"]}','{row["COMPLEMENTO"]}', '{row["COMPLEMENTO"]}', '{row["BAIRRO"]}',
                '{row["CIDADE"]}', '{row["ESTADO"]}', '{row["CEP"]}', '{row["LATITUDE"]}', '{row["LONGITUDE"]}',
                '{row["TELEFONE1"]}', '{row["SITE"]}', '{row["CNAE_DESCRICAO"]}', '{row["HORARIO_FUNCIONAMENTO"]}', 
                '{row["INSTAGRAM"]}', '{row["FACEBOOK"]}', '{row["OPCOES_DE_SERVICO"]}', '{row["SITUACAO_CADASTRAL"]}',
                {row["DATA_SITUACAO_CADASTRAL"]});""")
            except sqlite3.Error as e:
                print(f"Erro ao executar a query: {e}")
                logging.error(f"Erro ao executar a query: {e}")
                sys.exit(1)


# finaliza a transação
br_conn.commit() 
# Executa o comando VACUUM para compactar o banco de dados
br_conn.execute("VACUUM")
#en_conn.execute("VACUUM")

# Fecha a conexão com o banco de dados
br_conn.close()
#en_conn.close()

agora = datetime.now()
tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")
print(f"""A Base tem {contador_geral - contador_inativos} Registros Ativos
Foram inseridos {contador_novos} novos registros, 
Dos que existiam {contador_inativos} estão inativos.
""")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")
logging.info(f"""A Base tem {contador_geral - contador_inativos} Registros Ativos
Foram inseridos {contador_novos} novos registros, 
Dos que existiam {contador_inativos} estão inativos.
""")