"""libs"""
import time
import datetime
from fake_useragent import UserAgent
import ctypes
import gc
import re
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="src_teste.log",encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# pegando a resolução da tela

user32 = ctypes.windll.user32
resolucao = user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)


hoje = datetime.datetime.today().strftime("%B")
arquivocsv = f'{hoje}_Estabelecimentos_Alelo.csv'

"""
   Omite o Navegador na Execução
"""
chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument('--allow-insecure-localhost')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-software-rasterizer')
chrome_options.add_argument(f'--window-size={resolucao[0]},{resolucao[1]}')
chrome_options.add_argument('--log-level=3')

ua = UserAgent()
userAgent = ua.chrome


driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.implicitly_wait(15)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
driver.maximize_window()




opcoes = ['http://abrasel.com.br',
'https://ac.abrasel.com.br',
'https://agreste.abrasel.com.br',
'https://al.abrasel.com.br',
'https://amogisp.abrasel.com.br',
'https://ap.abrasel.com.br',
'https://am.abrasel.com.br',
'https://ba.abrasel.com.br',
'https://vertentes.abrasel.com.br',
'https://camposgerais.abrasel.com.br',
'https://cariri.abrasel.com.br',
'https://ce.abrasel.com.br',
'https://sma.abrasel.com.br',
'https://cspr.abrasel.com.br',
'https://centrosulro.abrasel.com.br',
'https://ba.abrasel.com.br',
'https://costalestems.abrasel.com.br',
'https://df.abrasel.com.br',
'https://es.abrasel.com.br',
'https://go.abrasel.com.br',
'https://hortensias.abrasel.com.br',
'https://jequitinhonhamg.abrasel.com.br',
'https://litopar.abrasel.com.br',
'https://ma.abrasel.com.br',
'https://mt.abrasel.com.br',
'https://ms.abrasel.com.br',
'https://mg.abrasel.com.br',
'https://nlrj.abrasel.com.br',
'https://noroestepr.abrasel.com.br',
'https://ndp.abrasel.com.br',
'https://odp.abrasel.com.br',
'https://orn.abrasel.com.br',
'https://pa.abrasel.com.br',
'https://pb.abrasel.com.br',
'https://pr.abrasel.com.br',
'https://pe.abrasel.com.br',
'https://pi.abrasel.com.br',
'https://piparn.abrasel.com.br',
'https://rmc.abrasel.com.br',
'https://agresteal.abrasel.com.br',
'https://nmg.abrasel.com.br',
'https://vrdmg.abrasel.com.br',
'https://rj.abrasel.com.br',
'https://rn.abrasel.com.br',
'https://rs.abrasel.com.br',
'https://ro.abrasel.com.br',
'https://rr.abrasel.com.br',
'https://sc.abrasel.com.br',
'https://sp.abrasel.com.br',
'https://se.abrasel.com.br',
'https://bonito.abrasel.com.br',
'https://serrasmg.abrasel.com.br',
'https://smg.abrasel.com.br',
'https://coma.abrasel.com.br',
'https://sfrj.abrasel.com.br',
'https://sulms.abrasel.com.br',
'https://to.abrasel.com.br',
'https://triangulomg.abrasel.com.br',
'https://uniaooestemg.abrasel.com.br',
'https://vdamg.abrasel.com.br',
'https://zm.abrasel.com.br']

lista = { "link":[], "formulário":[] , "link_formulario":[] }

for opcao in opcoes:
    driver.get(f'{opcao}/associe-se/')
    actions = ActionChains(driver, duration=100)
    time.sleep(0.5)
    try:
        botao = driver.find_element(By.CLASS_NAME,'btn-inscreva')
        link = botao.get_attribute('href')
        if 'https://siga.abrasel.com.br/' in link:
            lista['link'].append(opcao)
            lista['formulário'].append('Formulário Novo')
            lista['link_formulario'].append(link)
        else:
            lista['link'].append(opcao)
            lista['formulário'].append('Formulário Limbo')
            lista['link_formulario'].append(link)
    except:
        lista['link'].append(opcao)
        lista['formulário'].append('Sem formulário')
        lista['link_formulario'].append('')

dados = pd.DataFrame(lista)
dados.to_csv('Formulários Links.csv', index=False, sep=';', encoding='utf-8')