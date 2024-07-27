from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
import time
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)


def get_district_names(state_name):
    # Path to the Edge WebDriver executable
    webdriver_path = "C:/Users/ayan1/PycharmProjects/Webscraping/Task 2/msedgedriver.exe"

    # Check if the WebDriver path exists
    if not os.path.isfile(webdriver_path):
        logging.error(f"The WebDriver path is not valid: {webdriver_path}")
        return

    # Set up Edge options and driver
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    s = Service(webdriver_path)
    driver = webdriver.Edge(service=s, options=options)

    url = 'https://dashboard.pmjay.gov.in/pmj/#/'

    try:
        driver.get(url)
        time.sleep(2)
        wait = WebDriverWait(driver, 20)

        state_xpath = """/html/body/div/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[1]/div[1]/select"""
        district_xpath = """/html/body/div[1]/app-root/div/app-pmjay-home-dashboard/div/div/div[2]/div/div[1]/div/app-home-dashboard/div[1]/section[2]/div/div/div[19]/div/div[1]/div[2]/select"""

        # Select state
        state_select = Select(wait.until(EC.element_to_be_clickable((By.XPATH, state_xpath))))
        state_select.select_by_visible_text(state_name)
        time.sleep(1)

        # Get all district options
        district_select = Select(wait.until(EC.element_to_be_clickable((By.XPATH, district_xpath))))
        all_districts = [f'"{option.text}"' for option in district_select.options if
                         option.text not in ["Select District", ""]]

        # Print district names, comma-separated
        print(", ".join(all_districts))

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        time.sleep(5)
        driver.quit()


if __name__ == "__main__":
    state = "MADHYA PRADESH"  # Change this to the desired state name
    get_district_names(state)
