import os
import zipfile
from datetime import datetime

def backup_limpeza_com_data(dia:int = datetime.now().day, 
    mes:int = datetime.now().month, 
    ano:int = datetime.now().year, 
    pasta:str = os.listdir('.')[0], 
    nome_zipado:str = (f'backup_{str(datetime.now()).replace(":","-")}.zip'),
    extensao:str = '.csv'):
    """
    # Define a data de modificação desejada
    data_modificacao = datetime.datetime(ano, mes, dia)

    # Define o diretório de trabalho
    diretorio = 'caminho/do/diretorio'

    # Define o nome do arquivo zip a ser criado
    nome_zip = 'arquivo.zip'

    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w', compression=zipfile.ZIP_DEFLATED) as zipf:
        
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith('.csv'):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                data_mod = datetime.datetime.fromtimestamp(os.path.getmtime(caminho_arquivo))
                if data_mod.date() == data_modificacao.date():
                    
                    # Adiciona o arquivo ao arquivo zip
                    zipf.write(caminho_arquivo, arquivo)
                    
                    # Exclui o arquivo do diretório
                    os.remove(caminho_arquivo)
    """
    # Define a data de modificação desejada
    data_modificacao = datetime(ano, mes, dia)

    # Define o diretório de trabalho
    diretorio = pasta

    # Define o nome do arquivo zip a ser criado
    nome_zip = nome_zipado

    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w',compression=zipfile.ZIP_DEFLATED) as zipf:
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith(extensao):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                data_mod = datetime.fromtimestamp(os.path.getmtime(caminho_arquivo))
                if data_mod.date() == data_modificacao.date():
                    # Adiciona o arquivo ao arquivo zip
                    zipf.write(caminho_arquivo, arquivo)
                    # Exclui o arquivo do diretório
                    os.remove(caminho_arquivo)

def backup_limpeza_simples(pasta:str = os.listdir('.')[0], 
    nome_zipado:str = (f'backup_{str(datetime.now()).replace(":","-")}.zip'),
    extensao:str = '.csv'):
    """
    # Define o diretório de trabalho
    diretorio = pasta

    # Define o nome do arquivo zip a ser criado
    nome_zip = nome_zipado

    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w',compression=zipfile.ZIP_DEFLATED) as zipf:
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith(extensao):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                # Adiciona o arquivo ao arquivo zip
                zipf.write(caminho_arquivo, arquivo)
                # Exclui o arquivo do diretório
                os.remove(caminho_arquivo)
    """
    # Define o diretório de trabalho
    diretorio = pasta

    # Define o nome do arquivo zip a ser criado
    nome_zip = nome_zipado

    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w',compression=zipfile.ZIP_DEFLATED) as zipf:
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith(extensao):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                # Adiciona o arquivo ao arquivo zip
                zipf.write(caminho_arquivo, arquivo)
                # Exclui o arquivo do diretório
                os.remove(caminho_arquivo)