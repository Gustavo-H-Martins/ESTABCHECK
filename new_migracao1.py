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
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool, cpu_count


# verificando e gerando o backup dos dados. 
database = r'./database/'

# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

all_files_database = list(filter(lambda x: '.db' in x, os.listdir(database)))

if len(all_files_database) >= 1:
    backup_simples(diretorio_origem=database, nome_zipado=f"backup_database_{datazip}.zip", extensao='.db', diretorio_destino=f"{database}backup/")


# Define os diretórios
br_diretorio = r'./br_base/'

# Verifica se existe o diretorios
for diretorio in [database, br_diretorio]:
    existe_dir(diretorio)

def update_database(part):
    # gerando log
    logging.basicConfig(filename='logs\migracao.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    print(f"Início do processo: {agora.date()} às {agora.time()}")
    logging.info(f"Início do processo: {agora.date()} às {agora.time()}")
    contador_geral = 0 
    contador_novos = 0
    # Cria uma conexão com os SQLites
    with sqlite3.connect("./database/br_base_cnpj.db") as br_conn:
        for _, row in tqdm(part.iterrows(), total=part.shape[0]):
            contador_geral += 1
            result = br_conn.execute("SELECT CNPJ, SITUACAO_CADASTRAL, DATA_SITUACAO_CADASTRAL FROM estabelecimentos WHERE CNPJ=?", (row['CNPJ'],)).fetchone()

            if result is not None:
                try:
                    # linha já existe, fazer update
                    br_conn.execute("""
                        UPDATE estabelecimentos 
                        SET 
                            SITUACAO_CADASTRAL = CASE WHEN 
                                SITUACAO_CADASTRAL IN (2, 3, 4) THEN SITUACAO_CADASTRAL 
                                ELSE ? END, 
                            DATA_SITUACAO_CADASTRAL = CASE WHEN 
                                SITUACAO_CADASTRAL IN (2, 3, 4) THEN DATA_SITUACAO_CADASTRAL 
                                ELSE ? END
                        WHERE CNPJ=?
                    """, (row['SITUACAO_CADASTRAL'], row['DATA_SITUACAO_CADASTRAL'], row['CNPJ']))
                except sqlite3.Error as e:
                    print(f"Erro ao executar a query: {e}")
                    logging.error(f"Erro ao executar a query: {e}")
                    sys.exit(1)
            else:
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
                    ('{row["CNPJ"]}', '{row["RAZAO_SOCIAL"]}', '{row["NOME_FANTASIA"]}', '{row["RUA"]}',
                    '{row["NUMERO"]}','{row["COMPLEMENTO"]}', '{row["BAIRRO"]}', '{row["CIDADE"]}',
                    '{row["ESTADO"]}', '{row["CEP"]}', '{row["LATITUDE"]}', '{row["LONGITUDE"]}',
                    '{row["TELEFONE1"]}', '{row["SITE"]}', '{row["CNAE_DESCRICAO"]}',
                    '{row["HORARIO_FUNCIONAMENTO"]}', '{row["INSTAGRAM"]}', '{row["FACEBOOK"]}',
                    '{row["OPCOES_DE_SERVICO"]}', '{row["SITUACAO_CADASTRAL"]}',
                    '{row["DATA_SITUACAO_CADASTRAL"]}');
                    """)
                except sqlite3.Error as e:
                    print(f"Erro ao executar a query: {e}")
                    logging.error(f"Erro ao executar a query: {e}")
                    sys.exit(1)
    
    # finaliza a transação
    br_conn.commit()
    # Executa o comando VACUUM para compactar o banco de dados
    br_conn.execute("VACUUM")
    # Fecha a conexão com o banco de dados
    br_conn.close()



    agora = datetime.now()
    tempo_execucao = time.process_time()
    print(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao}!")
    print(f"""A Base tem {contador_geral} Registros Ativos
    Foram inseridos {contador_novos} novos registros,
    """)
    logging.info(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao}!")
    logging.info(f"""A Base tem {contador_geral} Registros Ativos
    Foram inseridos {contador_novos} novos registros,
    """)

    def update_establishments_data(df):
        chunk_size = int(np.ceil(len(df) / cpu_count()))
        parts = [df.iloc[i:i+chunk_size,:] for i in range(0, len(df), chunk_size)]
        with Pool(cpu_count()) as pool:
            pool.map(update_database, parts)

"""    #Exemplo de uso:
    df = pd.read_csv("dados_cadastrais.csv")
    update_establishments_data(df)"""