import glob
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import time
import zipfile
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import calendar
import time
import sys
from typing import Optional, Dict, Any, List
from database_connector import DatabaseConnector

class FEMSAAutomation:
    def __init__(self, cliente: str, db_connector: DatabaseConnector):
        self.cliente = cliente
        self.db = db_connector
        self.client_info = self.get_client_info()
        self.base_url = "https://femsab2b.bbr.cl"
        self.sales_dates = None
        self.available_options = []
        
        # Setup Chrome
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')  # Important for Ubuntu
        chrome_options.add_argument('--disable-dev-shm-usage')  # Important for Ubuntu
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--window-position=2000,0')
        
        # Additional flags to disable password alerts
        chrome_options.add_argument('--password-store=basic')
        chrome_options.add_argument('--no-default-browser-check')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-extensions')
        zip_path = self.get_download_path()  # Get the full zip path

        # Enhanced prefs to disable password manager and alerts
        prefs = {
            'download.default_directory': zip_path,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': False,  # Changed to False
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
            'profile.default_content_setting_values.notifications': 2,
            'profile.managed_default_content_settings.popups': 2,
            'autofill.profile_enabled': False,
            'profile.password_manager_enabled': False,
            'profile.default_content_settings.popups': 0,
            'profile.content_settings.exceptions.automatic_downloads.*.setting': 1,
            'profile.default_content_setting_values.automatic_downloads': 1
        }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)

    def get_client_info(self) -> Dict[str, Any]:
        """Get client information from database"""
        client_info = self.db.get_client_info(self.cliente)
        if not client_info:
            raise ValueError(f"No client information found for {self.cliente}")
        return client_info

    def get_download_path(self) -> str:
        """Get download path based on client name"""
        base_path = "/home/b2b_pharmatender/archivos_csv/carga"
        client_path = os.path.join(base_path, self.client_info['Nombre'], 'CRUZ_VERDE')
        zip_path = os.path.join(client_path, 'Zip')
        
        # Create directories if they don't exist
        os.makedirs(zip_path, exist_ok=True)
        return zip_path

    def get_extraction_path(self) -> str:
        """Get path for extracted files"""
        return os.path.dirname(self.get_download_path())

    def check_existing_report(self) -> bool:
        """Check if report was already generated for current date"""
        if not self.sales_dates:
            raise ValueError("Sales dates not yet scraped")
        return self.db.check_report_status(self.sales_dates['fecha'], self.cliente)
    

    def check_last_log_status(self):
        """Check if there's already a successful log entry for today"""
        try:
            query = """
                SELECT estado, updated_at 
                FROM log_script_carga_cadena_cliente 
                WHERE cliente = %s 
                AND cadena = 'cruz verde'
                AND DATE(updated_at) = CURDATE()
                AND estado = 1
                LIMIT 1
            """
            result = self.db.execute_query(query, (self.cliente,))
            
            if result and len(result) > 0:
                print(f"Found existing successful log for today: {result[0]}")
                return True
            
            print("No successful log found for today")
            return False
                
        except Exception as e:
            print(f"Error checking last log status: {str(e)}")
            return False
    
    def check_date_validity(self):
        """Check if scraped date is not too old"""
        try:
            if not self.sales_dates:
                raise ValueError("Sales dates not yet scraped")
                
            scraped_date = datetime.strptime(self.sales_dates['fecha'], '%Y-%m-%d').date()
            today = datetime.now().date()
            difference = today - scraped_date
            
            if difference.days > 2:
                print(f"Scraped date ({scraped_date}) is more than 2 days old from today ({today})")
                self.db.log_report_generation(self.cliente, 'cruz verde')
                self.db.update_report_status(self.cliente, 'cruz verde', 0)
                return False
                
            return True
            
        except Exception as e:
            print(f"Error checking date validity: {str(e)}")
            return False

    def login(self):
        """Login to FEMSA B2B platform"""
        try:
            self.driver.get(self.base_url)
            
            # Select Chile and Salud
            country_select = self.wait.until(EC.presence_of_element_located((By.ID, "pais")))
            country_select.send_keys("Chile")
            
            unit_select = self.wait.until(EC.presence_of_element_located((By.ID, "uneg")))
            unit_select.send_keys("Salud")
            
            # Click login button
            login_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn")))
            login_button.click()
            
            # Fill credentials
            username = self.wait.until(EC.presence_of_element_located((By.ID, "username")))
            password = self.wait.until(EC.presence_of_element_located((By.ID, "password")))
            
            username.send_keys(self.client_info['user'])
            password.send_keys(self.client_info['password'])
            
            submit_button = self.wait.until(EC.element_to_be_clickable((By.ID, "kc-login")))
            submit_button.click()
            
            print("Login successful")
            
            # Scrape sales dates after login
            self.scrape_sales_dates()
            
        except Exception as e:
            print(f"Login failed: {str(e)}")
            raise
    def logout(self):
        """Logout to clear the session """
        try:
            time.sleep(3)
            self.driver.switch_to.default_content()

            

            # Click filter button
            logout_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "btn-logout"))
            )
            logout_button.click()
            time.sleep(2)
            
            # Wait for download to complete
            time.sleep(5)
            print(f"Clicked logout button successfully")

        except Exception as e:
            print(f"Error downloading report: {str(e)}")
            raise


    def generate_reports(self):
        """Generate and download all required reports for both sales and inventory"""
        try:
            # Check if reports already exist
            if self.check_existing_report():
                print(f"Reports already generated for {self.sales_dates['fecha']}")
                return False

            # Log generation attempt
            self.db.log_report_generation(self.cliente, 'cruz verde')

            # Get number of iterations based on unidad_negocio
            iterations = self.client_info['unidad_negocio']
            
            # Process Sales Reports
            print("Starting Sales Reports Generation...")
            self.navigate_to_sales_report()
            
            for i in range(iterations):
                print(f"Processing sales report iteration {i+1}/{iterations}")
                
                if i == 0:
                    # First iteration follows normal flow
                    self.select_dropdown_option(i)
                    self.set_date_range()
                    self.download_report(i)
                    self.process_downloaded_files(i, 'ventas')  # Specify ventas
                else:
                    # Subsequent iterations use filter button
                    self.filter_button()
                    self.select_dropdown_option(i)
                    self.download_report(i)
                    self.process_downloaded_files(i, 'ventas')  # Specify ventas
                    time.sleep(1)
            
            # Process Inventory Reports
            print("Starting Inventory Reports Generation...")
            self.navigate_to_inventory_report()
            
            for i in range(iterations):
                print(f"Processing inventory report iteration {i+1}/{iterations}")
                
                if i == 0:
                    # First iteration follows normal flow
                    self.select_dropdown_option(i)
                    self.download_report2(i)
                    self.process_downloaded_files(i, 'inventario')  # Specify inventario
                else:
                    # Subsequent iterations use filter button
                    self.filter_button()
                    self.select_dropdown_option(i)
                    self.download_report2(i)
                    self.process_downloaded_files(i, 'inventario')  # Specify inventario
                    time.sleep(1)
            
            # Only update report status after both sales and inventory reports are complete
            print("All reports generated successfully")
            self.logout()
            self.db.update_report_status(self.cliente, 'cruz verde', 1)
            return True
            
        except Exception as e:
            print(f"Report generation failed: {str(e)}")
            # Log failure
            self.db.update_report_status(self.cliente, 'cruz verde', 0)
            raise
    def navigate_to_sales_report(self):
        """Navigate to sales report section"""
        try:
            # Wait for page to load after login
            time.sleep(3)
            
            def click_with_retry(selector, description, max_retries=3):
                """Helper function to implement click with retry mechanism"""
                for attempt in range(max_retries):
                    try:
                        element = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        time.sleep(1)
                        element.click()
                        return True
                    except Exception as e:
                        print(f"Attempt {attempt + 1} failed to click {description}: {str(e)}")
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(2)
                return False
            
            # Click menu button
            click_with_retry(".btn-menu-header", "menu button")
            
            # Wait for menu to expand
            time.sleep(2)
            
            # Click Reports menu item
            click_with_retry(
                ".bbr-menu-item:nth-child(4) > .bbr-menu-item__link",
                "reports menu"
            )
            time.sleep(2)
            
            # Click Sales Report option
            click_with_retry(
                ".bbr-menu-item:nth-child(1) > .bbr-menu-item__link",
                "sales report"
            )
            time.sleep(2)
            
            print("Successfully navigated to sales report")
            
        except Exception as e:
            print(f"Navigation failed: {str(e)}")
            raise
    def navigate_to_inventory_report(self):
        """Navigate to sales inventory section"""
        try:
            # Wait for page to load after login
            time.sleep(3)
            
            def click_with_retry(selector, description, max_retries=3):
                """Helper function to implement click with retry mechanism"""
                for attempt in range(max_retries):
                    try:
                        element = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        time.sleep(1)
                        element.click()
                        return True
                    except Exception as e:
                        print(f"Attempt {attempt + 1} failed to click {description}: {str(e)}")
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(2)
                return False
            
            # Click menu button
            click_with_retry(".btn-menu-header", "menu button")
            
            # Wait for menu to expand
            time.sleep(2)
            
            # Click Inventory Report option
            click_with_retry(
                ".bbr-menu-item:nth-child(2) > .bbr-menu-item__link",
                "inventory report"
            )
            time.sleep(2)
            
            print("Successfully navigated to inventory report")
            
        except Exception as e:
            print(f"Navigation failed: {str(e)}")
            raise

    def check_session_active(self):
        """Check if session is still active by looking for back-home element"""
        try:
            self.driver.switch_to.default_content()
            back_home = self.driver.find_elements(By.CSS_SELECTOR, ".back-home")
            if back_home:
                print("Session expired, detected back-home element")
                return False
            return True
        except Exception as e:
            print(f"Error checking session: {str(e)}")
            return False

    def restart_session(self, target_method):
        """Restart session and return to previous state"""
        print("Restarting session...")
        try:
            self.login()
            
            # Map of methods to their navigation functions
            navigation_map = {
                'get_dropdown_options': self.navigate_to_sales_report,
                'select_dropdown_option': self.navigate_to_sales_report,
                # Add other method mappings as needed
            }
            
            # Execute the appropriate navigation function
            if target_method in navigation_map:
                navigation_map[target_method]()
                print(f"Successfully navigated back to previous state for {target_method}")
                return True
            return False
        except Exception as e:
            print(f"Error restarting session: {str(e)}")
            return False

    def get_dropdown_options(self):
        """Retrieve all available options from the dropdown with session handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Check session status
                if not self.check_session_active():
                    print(f"Session expired, attempt {retry_count + 1}/{max_retries} to restart")
                    if not self.restart_session('get_dropdown_options'):
                        raise Exception("Failed to restart session")
                
                # Switch to frame and proceed with dropdown options
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame(0)
                
                # Find and click the select element to open dropdown
                select_element = self.wait.until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR, 
                        "vaadin-select.bbr-filter-fields.bbr-filter-select"
                    ))
                )
                select_element.click()
                time.sleep(2)

                # Get all vaadin-items
                items = self.wait.until(
                    EC.presence_of_all_elements_located((
                        By.CSS_SELECTOR,
                        "vaadin-item"
                    ))
                )

                # Store options information
                self.available_options = []
                for item in items:
                    try:
                        label = item.get_attribute('label')
                        value = item.get_attribute('value')
                        self.available_options.append({
                            'label': label,
                            'value': value,
                            'element': item
                        })
                        print(f"Found option: {label} (value: {value})")
                    except:
                        continue

                # Close dropdown by clicking outside
                self.driver.execute_script("arguments[0].click();", select_element)
                
                return self.available_options

            except Exception as e:
                print(f"Error in attempt {retry_count + 1}: {str(e)}")
                retry_count += 1
                if retry_count == max_retries:
                    raise Exception(f"Failed to get dropdown options after {max_retries} attempts")
                time.sleep(2)  # Wait before retrying

    def select_dropdown_option(self, index):
        """Select dropdown option by index with session handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Check session status
                if not self.check_session_active():
                    print(f"Session expired, attempt {retry_count + 1}/{max_retries} to restart")
                    if not self.restart_session('select_dropdown_option'):
                        raise Exception("Failed to restart session")
                
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame(0)
                time.sleep(2)
                
                if not self.available_options:
                    self.get_dropdown_options()

                if 0 <= index < len(self.available_options):
                    # Rest of the existing select_dropdown_option code...
                    # [Previous implementation remains the same]
                    return True
                else:
                    print(f"Invalid index {index}. Available options: 0-{len(self.available_options)-1}")
                    return False

            except Exception as e:
                print(f"Error in attempt {retry_count + 1}: {str(e)}")
                retry_count += 1
                if retry_count == max_retries:
                    raise Exception(f"Failed to select dropdown option after {max_retries} attempts")
                time.sleep(2)  # Wait before retrying
    def get_date_range(self, reference_date_str):
        """Calculate date range based on the reference date"""
        reference_date = datetime.strptime(reference_date_str, "%Y-%m-%d")
        
        # Calculate start date (30/31 days before reference date)
        first_day = reference_date.replace(day=1)
        last_day_prev_month = first_day - timedelta(days=1)
        days_in_month = calendar.monthrange(last_day_prev_month.year, last_day_prev_month.month)[1]
        
        start_date = reference_date - timedelta(days=days_in_month)
        
        return start_date.strftime("%Y-%m-%d"), reference_date_str

    def set_date_range(self):
        """Set the date range in the date pickers with date format conversion"""
        try:
            # Switch to the appropriate frame
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(0)
            time.sleep(2)
            
            print("Starting date range setting process...")
            start_date_orig, end_date_orig = self.get_date_range(self.sales_dates['fecha'])
            
            # Convert dates to YYYY-MM-DD format
            start_date = self.convert_date_format(start_date_orig)
            #end_date = self.convert_date_format(end_date_orig)
            
            print(f"Converted dates - Start: {start_date} (from {start_date_orig})")
            print(f"                  End: {end_date_orig} ")

            # Find date picker containers
            print("Locating date picker fields...")
            start_picker = self.wait.until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "vaadin-date-picker.bbr-filter-fields:first-of-type"
                ))
            )
            
            end_picker = self.wait.until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "vaadin-date-picker.bbr-filter-fields:nth-of-type(2)"
                ))
            )

            # JavaScript for setting dates with proper format
            set_date_script = """
            arguments[0].value = arguments[1];
            arguments[0].dispatchEvent(new CustomEvent('value-changed', {
                detail: { value: arguments[1] },
                bubbles: true,
                composed: true
            }));
            """

            # Set start date
            print(f"Setting start date to {start_date}")
            self.driver.execute_script(set_date_script, start_picker, start_date)
            time.sleep(1)

            # Set end date
            print(f"Setting end date to {end_date_orig}")
            self.driver.execute_script(set_date_script, end_picker, end_date_orig)
            time.sleep(1)

            print(f"Completed date range setting - Start: {start_date}, End: {end_date_orig}")

            # Verify the values were set
            start_value = self.driver.execute_script("return arguments[0].value;", start_picker)
            end_value = self.driver.execute_script("return arguments[0].value;", end_picker)
            
            print(f"Verification - Start date: {start_value}, End date: {end_value}")
            
            if start_value != start_date or end_value != end_date_orig:
                print("Warning: Date values don't match expected values.")
                print(f"Expected: {start_date} to {end_date_orig}")
                print(f"Got: {start_value} to {end_value}")
            
        except Exception as e:
            print(f"Error setting date range: {str(e)}")
            raise
    def filter_button(self):
        """Click the filter button to generate a second report on the second iteration"""
        try:
            time.sleep(3)
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(0)
            
 
            # Click filter button
            filter_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "btn-filter"))
            )
            filter_button.click()
            time.sleep(2)
            
            # Wait for download to complete
            time.sleep(5)
            print(f"Clicked filter button successfully")

        except Exception as e:
            print(f"Error downloading report: {str(e)}")
            raise
    def download_report(self, iteration: int):
        """Generate and download the report"""
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(0)
            
            # Click generate button
            generate_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "vaadin-button.filter-button"))
            )
            generate_button.click()
            time.sleep(3)
            
            # Click download button
            download_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "btn-download"))
            )
            download_button.click()
            time.sleep(2)
            
            # Select "Descargar reporte" option in zip format!
            download_report = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".vaadin-menu-item:nth-child(3) > .link-button"))
            )
            download_report.click()
            time.sleep(2)
            
            # Select CSV format
            csv_option = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "vaadin-radio-button:nth-child(2) > label"))
            )
            csv_option.click()
            time.sleep(2)
            
            # Click apply button
            apply_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".filter-apply-button"))
            )
            apply_button.click()
            
            # Handle the final CSV download
            self.driver.switch_to.default_content()
            time.sleep(5)
            
            # Find and click the CSV download link
            csv_link = self.wait.until(
                EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "venta_"))
            )
            csv_link.click()
            
            # Wait for download to complete
            time.sleep(5)
            print(f"Report {iteration + 1} downloaded successfully")

        except Exception as e:
            print(f"Error downloading report: {str(e)}")
            raise
    def download_report2(self, iteration: int):
        """Generate and download the inventory report"""
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(0)
            
            print("Waiting for generate button...")
            generate_button2 = WebDriverWait(self.driver, 30).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "vaadin-button.filter-button"))
            )
            time.sleep(2)
            self.driver.execute_script("arguments[0].click();", generate_button2)
            print("Generate button clicked")
            time.sleep(10)
            
            print("Waiting for download button...")
            download_button2 = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.ID, "btn-download"))
            )
            self.driver.execute_script("arguments[0].click();", download_button2)
            print("Download button clicked")
            time.sleep(5)
            
            print("Selecting download report option...")
            download_report2 = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".vaadin-menu-item:nth-child(1) > .link-button"))
            )
            self.driver.execute_script("arguments[0].click();", download_report2)
            print("Download report option clicked")
            time.sleep(5)
            
            print("Selecting CSV option...")
            csv_option2 = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "vaadin-radio-button:nth-child(2) > label"))
            )
            self.driver.execute_script("arguments[0].click();", csv_option2)
            print("CSV option clicked")
            time.sleep(3)
            
            print("Clicking apply button...")
            apply_button2 = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "vaadin-button.filter-apply-button"))
            )
            self.driver.execute_script("arguments[0].click();", apply_button2)
            print("Apply button clicked")
            
            self.driver.switch_to.default_content()
            time.sleep(15)
            
            print("Looking for CSV download link...")
            try:
                csv_link = WebDriverWait(self.driver, 45).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'detalleinventario_')]"))
                )
                self.driver.execute_script("arguments[0].click();", csv_link)
                print("CSV link clicked")
            except Exception as e:
                print("Error finding CSV link:", str(e))
                raise
            
            time.sleep(15)
            print(f"Inventory report {iteration + 1} download process completed")

        except Exception as e:
            print(f"Error downloading inventory report: {str(e)}")
            print("Current URL:", self.driver.current_url)
            raise
    def convert_date_format(self, date_str):
        """Convert date from DD-MM-YYYY to YYYY-MM-DD format"""
        try:
            # Parse the date from DD-MM-YYYY format
            date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            # Return it in YYYY-MM-DD format
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error converting date format: {str(e)}")
            return date_str    
    def scrape_sales_dates(self):
        try:
            time.sleep(3)
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(0)
            
            # Find content slots that contain Ventas and date info
            ventas_element = self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[@class='cell-text-align-left' and @title='Ventas']"))
            )
            
            # Get the date from vaadin-grid-cell-content-5
            fecha = self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//vaadin-grid-cell-content[@slot='vaadin-grid-cell-content-5']//div"))
            ).get_attribute('title')
            
            # Get ultima_carga from vaadin-grid-cell-content-6
            ultima_carga = self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//vaadin-grid-cell-content[@slot='vaadin-grid-cell-content-6']//div"))
            ).get_attribute('title')
            
            self.sales_dates = {
                'fecha': self.convert_date_format(fecha),
                'ultima_carga': ultima_carga.split()[0] if ' ' in ultima_carga else ultima_carga
            }
            
            print(f"Scraped dates - Fecha: {self.sales_dates['fecha']}, Ultima carga: {self.sales_dates['ultima_carga']}")
            
            self.driver.switch_to.default_content()
            
        except Exception as e:
            print(f"Error scraping sales dates: {str(e)}")
            raise

    def get_latest_zip(self) -> List[str]:
        """
        Get path of the single most recently downloaded zip file based on creation time.
        Returns a list with single path to maintain compatibility with existing code.
        """
        zip_dir = self.get_download_path()
        zip_files = glob.glob(os.path.join(zip_dir, '*.zip'))
        if not zip_files:
            raise FileNotFoundError("No zip files found in download directory")
        
        # Sort all zip files by creation time (newest first)
        sorted_zips = sorted(zip_files, key=lambda x: os.path.getctime(os.path.join(zip_dir, x)), reverse=True)
        
        # Get only the most recent file created within the last 160 seconds
        current_time = time.time()
        for zip_file in sorted_zips:
            if (current_time - os.path.getctime(os.path.join(zip_dir, zip_file))) < 160:
                print(f"Found most recent zip file: {zip_file}")
                # Return as single-item list to maintain compatibility
                return [os.path.join(zip_dir, zip_file)]
        
        raise FileNotFoundError("No recently created zip files found")

    def process_downloaded_files(self, iteration: int, report_type: str = 'ventas'):
        """
        Process downloaded files and rename according to client config
        
        Args:
            iteration: The current iteration number
            report_type: Type of report ('ventas' or 'inventario')
        """
        try:
            # Get file naming patterns from database for the current client
            file_patterns = self.db.execute_query("""
                SELECT unidad_negocio_id, archivo_venta, archivo_inventario 
                FROM cliente_unidad_negocio 
                WHERE cliente_id = %s
                ORDER BY unidad_negocio_id DESC
            """, (self.client_info['id'],))
            
            if not file_patterns:
                raise ValueError(f"No file patterns found for cliente_id={self.client_info['id']}")
            
            current_pattern = file_patterns[iteration]
            patterns = {
                'archivo_venta': current_pattern['archivo_venta'],
                'archivo_inventario': current_pattern['archivo_inventario']
            }
            
            print(f"Processing {report_type} files for unidad_negocio_id {current_pattern['unidad_negocio_id']}")
            print(f"Venta: {patterns['archivo_venta']}, Inventario: {patterns['archivo_inventario']}")
            
            date_str = self.sales_dates['fecha'].replace('-', '')
            zip_dir = self.get_download_path()
            extraction_dir = self.get_extraction_path()
            
            if report_type == 'ventas':
                # Get the latest zip files from current iteration
                latest_zips = self.get_latest_zip()
                
                for zip_path in latest_zips:
                    print(f"Processing ZIP file: {zip_path}")
                    
                    # Create a temporary extraction directory for this specific zip
                    temp_extraction_dir = os.path.join(extraction_dir, f'temp_{os.path.basename(zip_path)}')
                    os.makedirs(temp_extraction_dir, exist_ok=True)
                    
                    try:
                        # Extract only the latest zip to temporary directory
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            zip_ref.extractall(temp_extraction_dir)
                        
                        # Process files from temporary directory
                        for file in os.listdir(temp_extraction_dir):
                            if file.endswith('.csv') and 'venta' in file.lower():
                                old_path = os.path.join(temp_extraction_dir, file)
                                new_name = f"{patterns['archivo_venta']}{date_str}.csv"
                                new_path = os.path.join(extraction_dir, new_name)
                                
                                print(f"Moving and renaming {file} to {new_name}")
                                if os.path.exists(new_path):
                                    os.remove(new_path)
                                os.rename(old_path, new_path)
                    finally:
                        # Clean up temporary directory
                        if os.path.exists(temp_extraction_dir):
                            import shutil
                            shutil.rmtree(temp_extraction_dir)
                        
            else:
                # For inventory reports, handle the CSV directly
                time.sleep(5)  # Wait for download to complete
                
                # Get the latest CSV file
                csv_files = [f for f in os.listdir(zip_dir) if f.endswith('.csv')]
                if csv_files:
                    latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join(zip_dir, x)))
                    
                    if 'detalleinventario' in latest_csv.lower():
                        old_path = os.path.join(zip_dir, latest_csv)
                        new_name = f"{patterns['archivo_inventario']}{date_str}.csv"
                        new_path = os.path.join(extraction_dir, new_name)
                        
                        print(f"Moving and renaming {latest_csv} to {new_name}")
                        if os.path.exists(new_path):
                            os.remove(new_path)
                        os.rename(old_path, new_path)
            
            print(f"Successfully processed {report_type} files for iteration {iteration}")
            
        except Exception as e:
            print(f"Error processing {report_type} files: {str(e)}")
            raise
    def close(self):
        """Close browser and database connections"""
        if self.driver:
            self.driver.quit()
        self.db.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <cliente>")
        sys.exit(1)
        
    cliente = sys.argv[1]
    
    # Initialize database connection
    db = DatabaseConnector(
        host='localhost',
        user='b2b_user',
        password='OPjp%6kiyEfX',
        database='python'
    )
    
    automation = None
    try:
        # Initialize automation
        automation = FEMSAAutomation(cliente, db)
        
        # Check if report was already generated today
        if automation.check_last_log_status():
            print("Exiting: Report already generated successfully today")
            sys.exit(0)
        
        # Run automation
        automation.login()

        
        # Check if scraped date is valid
        if not automation.check_date_validity():
            print("Exiting: Scraped date is too old")
            sys.exit(1)
            
        automation.generate_reports()
        
        print("Report generation completed successfully")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if automation and automation.db:
            automation.db.update_report_status(cliente, 'cruz verde', 0)
    finally:
        if automation:
            automation.close()

if __name__ == "__main__":
    main()