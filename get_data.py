
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd
import os
from clean_data import final_prep
import threading

SUPPLIER_URL = os.getenv('SUPPLIER_URL')
LOGIN = os.getenv('LOGIN')
PASSWORD = os.getenv('PASSWORD')
URL = f'{SUPPLIER_URL}/it/register.html'

write_lock = threading.Lock()

file = 'private_repo/clean_data/new_bags.csv'

if os.path.exists(file):
    os.remove(file)


VENDORS = ['JIMMY CHOO', 'AZ FACTORY', 'GIVENCHY', 'VALENTINO GARAVANI',
       'GUCCI', 'MIU MIU', 'OFF-WHITE', 'DOLCE & GABBANA', 'BOYY',
       'GANNI', 'JACQUEMUS', 'CELINE', 'DIESEL', 'PAUL SMITH',
       'ISABEL MARANT', 'BALENCIAGA', 'BURBERRY', 'CHRISTIAN LOUBOUTIN',
       'BRUNELLO CUCINELLI', 'MARC JACOBS', 'MAX MARA', 'FENDI',
       'ALEXANDER MCQUEEN', 'BOTTEGA VENETA', 'STELLA MCCARTNEY', 'ETRO',
       'ACNE STUDIOS', "MAISON KITSUNE'", 'SAINT LAURENT', 'FERRAGAMO',
       'BALMAIN', 'ALANUI', 'CHLOÉ', 'LOEWE', "TOD'S", 'VALEXTRA',
       'BALLY', 'THE ATTICO', 'BENEDETTA BRUZZICHES', 'ZANELLATO',
       'ROGER VIVIER', 'TORY BURCH', 'JIL SANDER', 'EMPORIO ARMANI',
       'MUGLER', 'MM6', 'THE ROW', 'HOGAN', 'COPERNI', 'ALAÏA',
       'OTTOLINGER', 'PUCCI', 'ZIMMERMANN', 'SELF-PORTRAIT', 'MONCLER',
       'COURRÈGES', 'DSQUARED2', 'VERSACE', 'CASADEI', 'MAISON MARGIELA',
       'ARMARIUM', "THEMOIRE'", 'KENZO', 'LANVIN',
       'ADIDAS BY STELLA MCCARTNEY', 'GOLDEN GOOSE', 'AMINA MUADDI',
       'DKNY', 'CHLOÉ X ERES', 'JACQUEMUS RESORT', 'MICHAEL MICHAEL KORS',
       'ISSEY MIYAKE', 'KHAITE', "LOEWE PAULA'S IBIZA",
       'PLEATS PLEASE ISSEY MIYAKE', 'BAOBAO ISSEY MIYAKE', 'AMI PARIS',
       'GIUSEPPE DI MORABITO', 'BLUMARINE', 'HEREU', 'BY FAR',
       'VIVIENNE WESTWOOD', 'MANEBI', 'MARIA LA ROSA', 'ROSANTICA',
       'PALM ANGELS', 'CONVERSE X DRKSHDW', 'MANU ATELIER',
       'ANITA BILARDI', 'CHAMPION X RICK OWENS', 'CHICA', 'TOM FORD',
       'AZ FACTORY BY ESTER MANAS', 'DE SIENA', 'REBECCA MINKOFF',
       'FERRAGAMO CREATIONS', 'Y/PROJECT']

def translate_to_english(driver):
    """Trigger Chrome's built-in translation to English."""
    try:
        # Locate and click the "Translate" button if it appears
        translate_button = driver.find_element(By.XPATH, "//span[text()='Translate']")
        translate_button.click()
        print("Clicked on 'Translate' button.")
    except Exception as e:
        print("Translate button not found or already translated:", e)
    time.sleep(5)  # Allow time for translation to complete
    

def get_breadcrumb_data(driver):
    """Extract the full breadcrumb path as the product type."""
    breadcrumb = driver.find_elements(By.XPATH, '//ol[@class="breadcrumb"]//span[@itemprop="name"]')
    product_type = ",".join([item.text.strip() for item in breadcrumb])
    return product_type



def extract_product_details(product, driver, idx):
    """Extract details from a single product element."""
    try:
        # Extract basic product details from the catalog page
        vendor = product.find_element(By.XPATH, ".//span[@data-tema]").get_attribute("data-tema")
        sku = product.find_element(By.XPATH, ".//span[@data-tema]").get_attribute("data-name").replace(vendor, '').strip()
        product_type = product.find_element(By.XPATH, ".//span[@data-tema]").get_attribute("data-category3")
        color = product.find_element(By.XPATH, ".//span[@data-tema]").get_attribute("data-variant")
        collection = product.find_element(By.CLASS_NAME, "bollinostagione").text.strip()

        # Open the product link in a new tab
        product_link = product.find_element(By.CSS_SELECTOR, '.cotienifoto a')
        product_url = product_link.get_attribute('href')
        driver.execute_script(f"window.open('{product_url}', '_blank');")
        driver.switch_to.window(driver.window_handles[1])

        # Wait for the page to load and locate the main product element
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "bloccoh1")))
        
        try:
            price = driver.find_element(By.XPATH, '//*[@id="prezzidettaglioprezzoxx"]/span[@class="saldi"]').text.strip()
            discounted_price = driver.find_element(By.XPATH, '//*[@id="prezzidettaglioprezzoxx"]/span[@class="saldi2 saldiproduct"]').text.strip()
        except:
            price = ''
            discounted_price = ''
        
        # Extract sizes
        try:
            size_elements = driver.find_elements(By.XPATH, "//div[@class='tagliamobileform']")
            sizes = [element.text.strip() for element in size_elements]
        except:
            sizes = 'OUT OF STOCK'

        # Extract images
        try:
            image_container = driver.find_element(By.XPATH, '//*[@id="bloccofotodett"]/div[3]')
            dettagli_elements = image_container.find_elements(By.CLASS_NAME, 'dettagli')
            image_urls = [dettagli.find_element(By.TAG_NAME, 'a').get_attribute('href') for dettagli in dettagli_elements]
            images = ','.join(image_urls)
        except:
            images = ''
        
        # Update vendor information from the product detail page
        try:
            vendor = driver.find_element(By.XPATH, '//div[@id="bloccoh1"]/h1/a').text
        except:
            return

        # Find the accordion container and extract details
        product_details = extract_accordion_content(driver)
        
        # Extract product title and type
        try:
            product_title = driver.find_element(By.XPATH, '//*[@id="bloccoh1"]/p/font/font').text
        except:
            product_title = None

        # Close the product detail tab and switch back to the main tab
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

        # Store the extracted data into a DataFrame and save to CSV
        df = pd.DataFrame({
            'SKU': [sku],
            'Product Title': [product_title],
            'Product Type': [product_type],
            'Vendor': [vendor],
            'Price': [price],
            'Discounted Price': [discounted_price],
            'Collection': [collection],
            'Color': [color],
            'Tags': [','.join([product_type, color, collection])],
            'Size': [','.join(sizes)],
            'Images': [images],  
            'Description': [product_details.get('Description', None)],
            'Size & Fit': [product_details.get('Size & Fit', None)],
            'Made in': [product_details.get('Made in', None)],
            'Composition': [product_details.get('Composition', None)],
            'Tissue': [product_details.get('Tissue', None)]
        })
        
        with write_lock:
            df.to_csv(file, header=not os.path.exists(file), index=False, mode='a')
        
    except Exception as e:
        print(f"Error extracting product details: {e}")
    finally:
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

def extract_accordion_content(driver):
    """Extract the content from the accordion section of the product detail page."""
    try:
        accordion_container = driver.find_element(By.CLASS_NAME, "aks-accordion")
    except:
        return {}  # Return empty if no accordion is found
    
    accordion_items = accordion_container.find_elements(By.CLASS_NAME, "aks-accordion-item")
    product_details = {}
    for item in accordion_items:
        title = item.find_element(By.CLASS_NAME, "aks-accordion-item-title").text.strip()
        try:
            item.click()
            time.sleep(1)  
        except:
            pass  # Ignore if item is already expanded or can't be clicked

        try:
            content_element = item.find_element(By.CLASS_NAME, "aks-accordion-item-content")
            content = content_element.text.strip().replace('\n', ' ')
        except:
            content = ""
        product_details[title] = content

    return product_details

def setup_chrome_options():
    """Configure Chrome options for automatic translation and custom settings."""
    options = webdriver.ChromeOptions()  # or `webdriver.ChromeOptions()` if not using `undetected_chromedriver`
    
    # Basic settings for automatic translation and language preferences
    options.add_argument("--lang=en")  # Set default language to English
    prefs = {
        "translate_whitelists": {"it": "en"},  # Translate Italian to English automatically
        "translate": {"enabled": True}
    }
    options.add_experimental_option("prefs", prefs)
    
    # Additional options for headless mode and performance
    options.add_argument("--headless")  # Run Chrome in headless mode
    options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration (important for headless mode)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    return options

def parser(url, collection, pages, max_login_attempts=3):
    start, end, counter = None, None, None
    driver = None
    
    try:

        driver = webdriver.Chrome(options=setup_chrome_options())
        translate_to_english(driver) 
        
        login_attempts = 0
        logged_in = False
        
        while login_attempts < max_login_attempts:
            try:
                driver.get(URL)
                time.sleep(5)

                # Enter login details
                email_input = driver.find_element(By.ID, "UserID")
                email_input.send_keys(LOGIN)

                password_input = driver.find_element(By.ID, "passform3")
                password_input.send_keys(PASSWORD)
                password_input.send_keys(Keys.RETURN)

                # Wait for the page to load and check if login is successful by looking for the logout link
                time.sleep(5)
                
                try:
                    logout_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//a[@href='/sicurezza/logout.html']"))
                    )
                    logged_in = True
                    print("Login successful!")
                    break
                except:
                    print("Login failed, retrying...")

            except Exception as e:
                print(f"Login attempt {login_attempts + 1} failed: {e}")
            
            login_attempts += 1
            time.sleep(2)  # Wait a bit before retrying

        if not logged_in:
            print("Failed to login after multiple attempts. Exiting.")
            return
        
        start, end, counter = None, None, None
        
        if pages == 'all':
            url = url
        elif ',' in pages:
            start, end = pages.split(',')
            start, end = int(start), int(end)
            
            url = f'{url}?page={start}'
            counter = start
        else:
            url = f'{url}?page={pages}'
        
        driver.get(url)
        time.sleep(3)
        
        while True:
            try:
                catalog = driver.find_element(By.ID, "catalogogen")
                product_containers = catalog.find_elements(By.CLASS_NAME, "contfoto")
                
                if counter:
                    if counter > end:
                        break
                
                # Iterate through each product and extract details
                for idx, product in enumerate(product_containers, 1):
                    extract_product_details(product, driver, idx)
                
                try:
                    # Locate the pagination container
                    pagination = driver.find_element(By.CLASS_NAME, "bloccopagine")

                    # Get all <a> elements in the pagination list
                    page_links = pagination.find_elements(By.TAG_NAME, "a")

                    # Click on the second last <a> element
                    if len(page_links) > 1:
                        second_last_page_link = page_links[-2]
                        second_last_page_link.click()
                        time.sleep(5)  # Wait for the next page to load
                    else:
                        print("No more pages to click.")
                        break
                except:
                    print('Error pressing on next button...')
                    break
            except:
                print('No more products available...')
                break
            
            if start and end:
                counter += 1
    except:
        print('Error connecting to the website...')
        return
    finally:
        if driver:
            driver.quit()
            
if __name__ == "__main__":    
    start_time = time.time()
    collections = ['Bags'] * 6

    urls = [
        f'{SUPPLIER_URL}/it/borse-donna'
    ]  * 6
    
    pages = [
        '1,10',
        '11,20',
        '21,30',
        '31,40',
        '41,50',
        '51'
    ]

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(parser, url, collection, page) for url, collection, page in zip(urls, collections, pages)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")
    
    final_prep()
    
    end_time = time.time()  
    execution_time = end_time - start_time

    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    print(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    print(f"Total execution time: {execution_time:.2f} seconds")