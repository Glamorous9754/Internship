from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
import time
import logging
import pandas as pd
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Global variables
METADATA_FILE = 'metadata_pmjay.xlsx'
EXPECTED_COLUMNS = ["State", "District", "Hospital"]


# Function to safely interact with elements
def safe_interact(driver, xpath, interaction, value=None):
    retries = 5
    while retries > 0:
        try:
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
            if interaction == 'click':
                element.click()
            elif interaction == 'select':
                Select(element).select_by_visible_text(value)
            return element  # Return the element for further use if needed
        except Exception as e:
            logging.warning(f"Retrying interaction due to: {e}")
            time.sleep(1)
            retries -= 1
    logging.error(f"Failed to {interaction} the element after retries")


# Function to save metadata to Excel
def save_metadata_to_excel(state_name, district_name, hospital_name):
    metadata = pd.DataFrame([[state_name, district_name, hospital_name]], columns=EXPECTED_COLUMNS)
    if os.path.exists(METADATA_FILE):
        existing_data = pd.read_excel(METADATA_FILE)
        metadata = pd.concat([existing_data, metadata], ignore_index=True)
    metadata.to_excel(METADATA_FILE, index=False)
    logging.info(f"Metadata saved for {hospital_name}: {state_name}, {district_name}")


# Main function to run the scraping process
def main(state_name=None):
    # Set up Edge options and driver
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    s = Service("/msedgedriver.exe")
    driver = webdriver.Edge(service=s, options=options)

    url = 'https://dashboard.pmjay.gov.in/pmj/#/'

    try:
        driver.get(url)
        time.sleep(2)
        wait = WebDriverWait(driver, 20)

        state_xpath = """/html/body/div/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[1]/div[1]/select"""
        district_xpath = """/html/body/div[1]/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[1]/div[2]/select"""
        hospital_xpath = """/html/body/div[1]/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[1]/div[3]/select"""

        # Select state
        if state_name:
            state_select = Select(wait.until(EC.element_to_be_clickable((By.XPATH, state_xpath))))
            state_select.select_by_visible_text(state_name)
            time.sleep(1)

        # Select hospitals in the state
        district_select = Select(wait.until(EC.element_to_be_clickable((By.XPATH, district_xpath))))
        all_districts = district_select.options

        for district_option in all_districts:
            if district_option.text in ["Select District", ""]:
                continue

            district_select.select_by_visible_text(district_option.text)
            time.sleep(1)

            hospital_select = Select(wait.until(EC.element_to_be_clickable((By.XPATH, hospital_xpath))))
            all_hospitals = hospital_select.options

            for hospital_option in all_hospitals:
                if hospital_option.text in ["Select Hospital", ""]:
                    continue

                hospital_select.select_by_visible_text(hospital_option.text)
                time.sleep(1)

                button_xpath = """//*[@id="Skip-Main-Content"]/section[2]/div/div/div[19]/div/div[2]/button[1]"""
                safe_interact(driver, button_xpath, 'click')
                time.sleep(5)

                try:
                    download_button_xpath = """/html/body/div[1]/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[2]/button[2]"""
                    safe_interact(driver, download_button_xpath, 'click')
                    time.sleep(2)

                    excel_button_xpath = """/html/body/div[1]/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[2]/div/a[1]"""
                    safe_interact(driver, excel_button_xpath, 'click')
                    time.sleep(2)

                    # Save metadata to Excel file
                    save_metadata_to_excel(state_name, district_option.text, hospital_option.text)

                except Exception as e:
                    logging.error(f"Error downloading data for {hospital_option.text}: {e}")

    except Exception as e:
        logging.error(f"An error occurred during scraping: {e}")

    finally:
        time.sleep(10)
        driver.quit()


if __name__ == "__main__":
    state_name = input("Enter the state name: ").strip()
    main(state_name)
