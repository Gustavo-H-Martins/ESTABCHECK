import sys
import os
import logging
def existe_dir (dir_input:str = ""):
    if not os.path.exists(dir_input):
        print(f"Diretório de entrada {dir_input} não existe!")
        logging.error(f"Diretório de entrada {dir_input} não existe!")
        sys.exit(1)

def exist_file(file_input:str = ""):
    if not os.path.exists(file_input):
        print(f"Arquivo de entrada {file_input} não existe!")
        logging.error(f"Arquivo de entrada {file_input} não existe!")
        sys.exit(1)
        
def check_extensao(file_input:str = "", extensao:str = ".csv"):
    if not os.path.splitext(file_input)[1] == extensao:
        print(f"Arquivo de entrada {file_input} não está no formato correto!")
        logging.error(f"Arquivo de entrada {file_input} não está no formato correto!")
        sys.exit(1)