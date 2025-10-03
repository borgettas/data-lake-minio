import requests
import logging
from typing import List, Dict, Any


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def fetch_data(endpoint: str, page_size: int = 50) -> List[Dict[str, Any]]:
    
    url = endpoint
    all_data = []
    page = 1
    
    log.info(f"Start request for {url}")

    while True:
        try:
            params = {"per_page": page_size, "page": page}
            response = requests.get(url, params=params, timeout=15)
            # for err 4xx/5xx
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                log.info(f"Requests concluded")
                break
            
            all_data.extend(data)
            log.info(f"Page '{page}' loaded. Total: {len(all_data)}")
            page += 1
            
        except requests.exceptions.Timeout:
            log.error(f"Timeout: {url}")
            raise requests.exceptions.RequestException(f"Timeout: {url}")
        except requests.exceptions.RequestException as e:
            # err 4xx/5xx and more
            log.error(f"Error opn request, page '{page}': {e}")
            raise requests.exceptions.RequestException(f"Error on request, page '{page}': {e}")
            
    return all_data


# if __name__ == "__main__":
#     try:
#         breweries = fetch_data("/breweries")
#         log.info(f"\Extraction finished. Total of brewerys: {len(breweries)}")
#     except requests.exceptions.RequestException as e:
#         log.error(f"Fail on extraction: {e}")