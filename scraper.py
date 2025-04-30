from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

# Configuração do diretório de download
download_dir = os.path.abspath("dados_combustiveis")

# Configuração do Chrome para baixar arquivos automaticamente
chrome_options = Options()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Configuração do WebDriver
driver_path = "C:\\Users\\mateu\\chromedriver.exe"  # Atualize conforme necessário
service = Service(driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

# Acessa a página
url = "https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp"
driver.get(url)
time.sleep(5)  # Espera a página carregar

# Encontra todos os botões de download
download_buttons = driver.find_elements(By.ID, "btnDownloadUrl")

if not download_buttons:
    print("Nenhum botão de download encontrado! Verifique o XPath.")
else:
    print(f"{len(download_buttons)} botões de download encontrados!")

# Clica nos botões garantindo que estão visíveis e clicáveis
for button in download_buttons:
    try:
        # Rola até o botão estar visível
        driver.execute_script("arguments[0].scrollIntoView();", button)
        time.sleep(2)  # Espera um pouco para evitar erro de carregamento
        
        # Aguarda até que o botão seja clicável
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(button))

        print("Clicando no botão de download...")
        button.click()
        time.sleep(5)  # Espera o download começar

    except Exception as e:
        print(f"Erro ao clicar no botão: {e}")

# Tempo extra para garantir o download
time.sleep(10)

# Fecha o navegador
driver.quit()

print(f"Arquivos baixados para: {download_dir}")