import json
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import concurrent.futures
import time
from tqdm import tqdm
import datetime
import os

# Load URL list from file
with open("url_list.json", "r") as f:
    urllist = json.load(f)


# Retry mechanism decorator
def retry(times):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    time.sleep(2)
            raise last_exception

        return wrapper

    return decorator


# Fetch URL with retry
@retry(4)
def fetch_url(url):
    return requests.get(url)


# Extract Headers for Each Table (only once)
def extract_headers(urllist):
    response = fetch_url(urllist[0])
    soup = bs(response.content, 'html.parser')

    header1 = soup.find_all('tr')[0].text.split('\n')[1:6]

    cards = soup.find_all('div', class_='card')

    heads2 = cards[0].find('thead').find_all('tr')
    head1_2 = heads2[0].find_all('th')
    head2_2 = heads2[2].find_all('th')
    head3_2 = heads2[3].find_all('th')
    headers2 = [f"{head3_2[l].text.strip()} - {head2_2[k].text.strip()} - {head1_2[j].text.strip()}"
                for j in range(2) for k in range(2) for l in range(4)]

    heads3 = cards[1].find_all('tr')
    head1_3 = 'Planned outlay'
    head2_3 = 'Scheme'
    head3_3 = heads3[2].find_all('th')
    head4_3 = heads3[3].find_all('th')
    headers3 = ['S.No.', 'Sector'] + [f"{head4_3[k].text.strip()} - {head3_3[j].text.strip()} - {head2_3} - {head1_3}"
                                      for j in range(1) for k in range(8)]

    heads4 = cards[2].find('thead').find_all('tr')
    head1_4 = heads4[0].find_all('th')
    head2_4 = heads4[1].find_all('th')
    head3_4 = heads4[2].find_all('th')
    headers4 = [head1_4[0].text.strip(), head1_4[1].text.strip(), head1_4[2].text.strip()] + [
        f"{head3_4[l].text.strip()} - {head2_4[k].text.strip()} - {head1_4[j].text.strip()}"
        for j in range(3, 5) for k in range(2) for l in range(4)]

    headers5 = [th.get_text(strip=True) for th in cards[3].find('thead').find_all('tr')[0].find_all('th')]

    return header1, headers2, headers3, headers4, headers5


# Extract Row Data for Each Table
def extract_rows_first_table(soup):
    row = soup.find('tr', class_="tblRowB")
    if row:
        row_data = row.text.split('\n')
        row_data = [data.strip() for data in row_data if data.strip()]
        if len(row_data) >= 5:
            return row_data
    return []


def extract_rows_second_table(soup):
    rows = soup.find_all('div', class_='card')[0].find('tbody').find_all('tr')
    data = [[cell.get_text(strip=True) for cell in row.find_all("td")] for row in rows]
    return data


def extract_rows_third_table(soup):
    rows = soup.find_all('div', class_='card')[1].find('tbody').find_all('tr')
    data = [[cell.get_text(strip=True) for cell in row.find_all(["th", "td"])] for row in rows]
    return data


def extract_rows_fourth_table(soup):
    rows = soup.find_all('div', class_='card')[2].find('tbody').find_all('tr')
    data = [[cell.get_text(strip=True) for cell in row.find_all("td")] for row in rows]
    return data


def extract_rows_fifth_table(soup):
    rows = soup.find_all('div', class_='card')[3].find('tbody').find_all('tr')
    data = [[cell.get_text(strip=True) for cell in row.find_all("td")] for row in rows]
    return data


# Extract Data for Each Table with Headers
def extract_data_from_url(url):
    try:
        response = fetch_url(url)
        soup = bs(response.content, 'html.parser')

        row_data1 = extract_rows_first_table(soup)
        data2 = extract_rows_second_table(soup)
        data3 = extract_rows_third_table(soup)
        data4 = extract_rows_fourth_table(soup)
        data5 = extract_rows_fifth_table(soup)

        return url, row_data1, data2, data3, data4, data5
    except Exception as e:
        return url, None, None, None, None, None


def extract_data(urllist, headers1, headers2, headers3, headers4, headers5):
    data1, data2, data3, data4, data5 = [], [], [], [], []
    row_count2, row_count3, row_count4, row_count5 = [], [], [], []

    checkpoint_file = "../checkpoint.txt"
    start_index = 0

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            start_index = int(f.read().strip())

    if os.path.exists("Approved_Action_Plan_Report.csv"):
        data1 = pd.read_csv("Approved_Action_Plan_Report.csv").values.tolist()
    if os.path.exists("Section_1_Plan_Summary.csv"):
        data2 = pd.read_csv("Section_1_Plan_Summary.csv").values.tolist()
    if os.path.exists("Section_2_Sectoral_View.csv"):
        data3 = pd.read_csv("Section_2_Sectoral_View.csv").values.tolist()
    if os.path.exists("Section_3_Scheme_View.csv"):
        data4 = pd.read_csv("Section_3_Scheme_View.csv").values.tolist()
    if os.path.exists("Section_4_Priority_Wise_Activity_Details.csv"):
        data5 = pd.read_csv("Section_4_Priority_Wise_Activity_Details.csv").values.tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(extract_data_from_url, url): url for url in urllist[start_index:]}
        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urllist[start_index:]),
                           desc="Processing URLs"):
            url, row_data1, data2_rows, data3_rows, data4_rows, data5_rows = future.result()
            if row_data1:
                data1.append(row_data1)
            if data2_rows:
                data2.extend(data2_rows)
                row_count2.append(len(data2_rows))
            if data3_rows:
                data3.extend(data3_rows)
                row_count3.append(len(data3_rows))
            if data4_rows:
                data4.extend(data4_rows)
                row_count4.append(len(data4_rows))
            if data5_rows:
                data5.extend(data5_rows)
                row_count5.append(len(data5_rows))

            # Save intermediate results
            save_intermediate_data(data1, data2, data3, data4, data5, headers1, headers2, headers3, headers4, headers5)

            # Update checkpoint
            with open(checkpoint_file, "w") as f:
                f.write(str(urllist.index(url) + 1))

    return data1, data2, data3, data4, data5, row_count2, row_count3, row_count4, row_count5


# Save intermediate data to CSV
def save_intermediate_data(data1, data2, data3, data4, data5, headers1, headers2, headers3, headers4, headers5):
    table1 = pd.DataFrame(data1, columns=headers1)
    table2 = pd.DataFrame(data2, columns=headers2)
    table3 = pd.DataFrame(data3, columns=headers3)
    table4 = pd.DataFrame(data4, columns=headers4)
    table5 = pd.DataFrame(data5, columns=headers5)

    table1.to_csv('Approved_Action_Plan_Report.csv', index=False)
    table2.to_csv('Section_1_Plan_Summary.csv', index=False)
    table3.to_csv('Section_2_Sectoral_View.csv', index=False)
    table4.to_csv('Section_3_Scheme_View.csv', index=False)
    table5.to_csv('Section_4_Priority_Wise_Activity_Details.csv', index=False)


# Create and Save DataFrames
def create_and_save_dataframes(data1, data2, data3, data4, data5, row_count2, row_count3, row_count4, row_count5,
                               headers1, headers2, headers3, headers4, headers5):
    # Current date and time for naming files
    now = datetime.datetime.now()
    current_time = now.strftime("%Y%m%d_%H%M%S")

    # Combine headers
    combined_headers2 = headers1 + headers2
    combined_headers3 = headers1 + headers3
    combined_headers4 = headers1 + headers4
    combined_headers5 = headers1 + headers5

    # Combine data
    combined_data2 = []
    combined_data3 = []
    combined_data4 = []
    combined_data5 = []

    for row_data1, count2, count3, count4, count5 in zip(data1, row_count2, row_count3, row_count4, row_count5):
        combined_data2.extend([row_data1 + row for row in data2[:count2]])
        combined_data3.extend([row_data1 + row for row in data3[:count3]])
        combined_data4.extend([row_data1 + row for row in data4[:count4]])
        combined_data5.extend([row_data1 + row for row in data5[:count5]])
        data2 = data2[count2:]
        data3 = data3[count3:]
        data4 = data4[count4:]
        data5 = data5[count5:]

    # Create DataFrames
    table1 = pd.DataFrame(data1, columns=headers1)
    table2 = pd.DataFrame(combined_data2, columns=combined_headers2)
    table3 = pd.DataFrame(combined_data3, columns=combined_headers3)
    table4 = pd.DataFrame(combined_data4, columns=combined_headers4)
    table5 = pd.DataFrame(combined_data5, columns=combined_headers5)

    # Save to CSV
    table1.to_csv(f'Approved_Action_Plan_Report_{current_time}.csv', index=False)
    table2.to_csv(f'Section_1_Plan_Summary_{current_time}.csv', index=False)
    table3.to_csv(f'Section_2_Sectoral_View_{current_time}.csv', index=False)
    table4.to_csv(f'Section_3_Scheme_View_{current_time}.csv', index=False)
    table5.to_csv(f'Section_4_Priority_Wise_Activity_Details_{current_time}.csv', index=False)


# Main Execution
if __name__ == "__main__":
    headers1, headers2, headers3, headers4, headers5 = extract_headers(urllist)
    data1, data2, data3, data4, data5, row_count2, row_count3, row_count4, row_count5 = extract_data(urllist, headers1,
                                                                                                     headers2, headers3,
                                                                                                     headers4, headers5)
    create_and_save_dataframes(data1, data2, data3, data4, data5, row_count2, row_count3, row_count4, row_count5,
                               headers1, headers2, headers3, headers4, headers5)
