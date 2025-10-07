# migrations/tests/test_api_connector.py
import pytest
import requests
import requests_mock
from migrations.scripts.api_connector import fetch_data, API_BASE_URL

# --- Dados Mock (Simulados) ---
# Resposta da Página 1
MOCK_DATA_PAGE_1 = [{"id": 1, "name": "Test Brew 1", "state": "CA"}] * 50
# Resposta da Página 2
MOCK_DATA_PAGE_2 = [{"id": 51, "name": "Test Brew 51", "state": "NY"}] * 20 
# Resposta da última página (vazia)
MOCK_DATA_EMPTY = []

def test_fetch_success_and_pagination(requests_mock):
    """Testa a extração bem-sucedida e se a paginação funciona."""
    endpoint = "/breweries"
    url = f"{API_BASE_URL}{endpoint}"
    
    # 1. Configura as respostas simuladas da API:
    # Página 1 (com dados)
    requests_mock.get(url, json=MOCK_DATA_PAGE_1, status_code=200, params={'per_page': '50', 'page': '1'})
    # Página 2 (com dados)
    requests_mock.get(url, json=MOCK_DATA_PAGE_2, status_code=200, params={'per_page': '50', 'page': '2'})
    # Página 3 (vazia, sinaliza o fim)
    requests_mock.get(url, json=MOCK_DATA_EMPTY, status_code=200, params={'per_page': '50', 'page': '3'})
    
    # 2. Executa a função
    result = fetch_data(endpoint, page_size=50)
    
    # 3. Verifica o resultado
    expected_total = len(MOCK_DATA_PAGE_1) + len(MOCK_DATA_PAGE_2)
    assert len(result) == expected_total
    assert result[0]['id'] == 1
    assert result[-1]['id'] == 51

def test_http_error_handling(requests_mock):
    """Testa se a função levanta exceção em caso de erro HTTP (ex: 500)."""
    endpoint = "/breweries"
    url = f"{API_BASE_URL}{endpoint}"
    
    # Simula um erro do servidor (500 Internal Server Error) na primeira página
    requests_mock.get(url, status_code=500, params={'per_page': '50', 'page': '1'})
    
    # Espera que uma exceção requests.exceptions.RequestException seja levantada
    with pytest.raises(requests.exceptions.RequestException) as excinfo:
        fetch_data(endpoint)
        
    assert "Erro na requisição da API na página 1" in str(excinfo.value)
    
def test_timeout_error(requests_mock):
    """Testa se a função levanta exceção em caso de timeout."""
    endpoint = "/breweries"
    url = f"{API_BASE_URL}{endpoint}"
    
    # Simula um Timeout
    requests_mock.get(url, exc=requests.exceptions.Timeout)
    
    # Espera que uma exceção requests.exceptions.RequestException seja levantada
    with pytest.raises(requests.exceptions.RequestException) as excinfo:
        fetch_data(endpoint)
        
    assert "Timeout ao acessar a API" in str(excinfo.value)