"""libs"""
import re
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import multiprocessing

merge_dados_dados = pd.read_json("recorte_apple_abrasel.json", dtype="string")

urls = merge_dados_dados["URL"].to_list()
def get_horario_servicos(url:str):
    #Omite o Navegador na Execução
    chrome_options = Options()
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--window-size=1366,768')
    chrome_options.add_argument('--log-level=3')

    # instanciando o navegador
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)
    driver.implicitly_wait(5)
    driver.maximize_window()
    actions = ActionChains(driver)
    try:
        # interagindo com a página
        actions.move_to_element(driver.find_element(By.CLASS_NAME, 'hfpxzc')).click().perform()
    except:
        pass
    try:
        # tentando clicar na tabela de horário
        actions.move_to_element(driver.find_element(By.CSS_SELECTOR, ".OMl5r.hH0dDd.jBYmhd")).click().perform()
        try:
            # tenta pegar os dados de horário de funcionamento
            tabela_horario = driver.find_element(By.TAG_NAME,'table')

            # coloca tudo na string quadro_horario
            quadro_horario = tabela_horario.text

            # Remove os parênteses e o que está dentro deles
            quadro_horario = re.sub(r"\(.*?\)", "", quadro_horario)

            # Remove as linhas que começam com letras maiúsculas
            quadro_horario = re.sub(r"^[O].*", "", quadro_horario, flags=re.MULTILINE)


            # Remove as linhas em branco extras
            quadro_horario = re.sub(r"\n+", "\n", quadro_horario)
            # print(quadro_horario)
        except:
            quadro_horario = ""
            pass
        try:
            # tentando clicar na tabela de opções de serviços
            actions.move_to_element(driver.find_element(By.CLASS_NAME, "PYvSYb")).click().perform()
            try:
                # tenta pegar os dados de opções de serviços
                tabela_opcoes_servico = driver.find_element(By.CLASS_NAME, 'iP2t7d')

                # coloca tudo na string opcoes_servico
                opcoes_servico = tabela_opcoes_servico.text
                #print(opcoes_servico)
            except: 
                opcoes_servico = ""
                pass
        except: 
            # tentando clicar na tabela de opções de serviços
            actions.move_to_element(driver.find_element(By.CLASS_NAME, "E0DTEd")).click().perform()
            try:
                # tenta pegar os dados de opções de serviços
                tabela_opcoes_servico = driver.find_element(By.CLASS_NAME, 'iP2t7d')

                # coloca tudo na string opcoes_servico
                opcoes_servico = tabela_opcoes_servico.text
                #print(opcoes_servico)
            except: 
                opcoes_servico = ""
                pass
        print("ok")
    except:
        quadro_horario = ""
        opcoes_servico = ""
        print(f"error : {url}")
        pass
    driver.close()

    # extrai os valores da lista de resultados
    complemento = {"HORARIO_FUNCIONAMENTO":[], "OPCOES_DE_SERVICO": [], "URL": []}
    complemento["HORARIO_FUNCIONAMENTO"].append(quadro_horario)
    complemento["OPCOES_DE_SERVICO"].append(opcoes_servico)
    complemento["URL"].append(url)
    dataframe = pd.DataFrame(complemento)
    dataframe.to_csv("recorte_apple_abrasel_complemento.csv", sep=';', encoding="utf-8", mode="a", index=False, header=False)

if __name__ == '__main__':
    # cria um pool com 6 processos
    pool = multiprocessing.Pool(5) 
    
    # aplica a função a cada url da lista
    pool.map(get_horario_servicos, urls[4700:]) 
    
    # fecha o pool
    pool.close() 
    # espera os processos terminarem
    pool.join() 