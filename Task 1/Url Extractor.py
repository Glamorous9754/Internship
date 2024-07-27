import requests
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# Function to make HTTP requests with retries
def get_with_retries(url):
    session = requests.Session()
    retry = requests.adapters.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(url)
        response.raise_for_status()
        return response

    except requests.RequestException:
        return None


def check_url_for_table(url):
    response = get_with_retries(url)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        return len(tables) > 0  # Return True if tables are found
    return False


def process_url(i, base_url, output_file):
    url = base_url.replace('\'\'\'i\'\'\'', str(i))
    if check_url_for_table(url):
        with open(output_file, 'a') as f:
            f.write(f'"{url}",\n')
    return None


def collect_urls_with_tables(start, end, base_url, output_file, max_entries):
    count = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_url, i, base_url, output_file): i for i in range(start, end + 1)}
        with tqdm(total=end - start + 1) as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result:
                    count += 1
                    if count >= max_entries:
                        break
                pbar.update(1)  # Update progress bar


if __name__ == "__main__":
    base_url = "https://egramswaraj.gov.in/webservice/approvedActionPlanExternalReport/'''i'''/2024-2025"
    output_file = 'urls_with_tables.json'
    start = 300000
    end = 350000
    max_entries = 400000

    # Ensure the output file is empty at the start
    with open(output_file, 'w') as f:
        f.write('')

    collect_urls_with_tables(start, end, base_url, output_file, max_entries)
