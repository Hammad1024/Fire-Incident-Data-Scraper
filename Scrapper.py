from datetime import date, datetime
import os
import pandas as pd
import re
import logging
import boto3
from io import StringIO
import requests
from bs4 import BeautifulSoup

logging.basicConfig(filename='cost_of_natural_disaster.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

download_folder = '116.02 Cost of natural disaster/'
pipeline_folder = 'cost_of_natural_disaster/'

def get_s3_credentials():
    return {
        'aws_access_key_id': os.environ.get("S3_ACCESS_KEY_ID"),
        'aws_secret_access_key': os.environ.get("S3_SECRET_ACCESS_KEY")
    }

def get_pcloud_token():
    return os.environ.get("PCLOUD_TOKEN")

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"Fetched HTML from {url}")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch HTML from {url}: {e}")
        return None

def parse_date(date_str):
    date_match = re.match(r"(\d{4})\s(\w{3})\s(\d{1,2})(?:\s-\s(\d{1,2}))?", date_str)
    if date_match:
        year, month, start_day, end_day = date_match.groups()
        event_duration = int(end_day or start_day) - int(start_day) + 1
        starting_date = f"{start_day}/{month}/{year}"
        return event_duration, starting_date
    return None, None

def checkdate(date_str):
    words = date_str.split()
    if len(words) == 3 and words[0].isdigit() and words[2].isalpha():
        words[1], words[2] = words[2], words[1]
        result_string = ' '.join(words)
    else:
        result_string = ' '.join(words)
    return result_string

def extract_data(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="table_1")
        rows = table.tbody.find_all("tr")
    
        table_data = []
        
        for row in rows:
            columns = row.find_all("td")
            if len(columns) >= 5:
                event_date, event_name, event_categories, event_cost, inflation_cost = [col.get_text(strip=True) for col in columns[1:6]]
                date = checkdate(event_date)
                table_data.append({
                    "Date": date,
                    "Event": event_name.title(),
                    "Categories": event_categories.title(),
                    "Value (Cost, $m)": event_cost,
                    "Value (Inflation Adjusted Cost, $m)": inflation_cost
                })
        
        logging.info("Data extraction completed successfully")
        return table_data
    except Exception as e:
        logging.error(f"Failed to extract data: {e}")
        return None

def save_raw_data(csv_data):
    filename = "Cost Of Natural Disaster"
    df = pd.DataFrame(csv_data).convert_dtypes()
    logging.info("Data cleaning completed successfully")
    save_to_csv(df, download_folder, filename)

def clean_dataframe(csv_data):
    filename = "cost_of_natural_disaster"
    
    if csv_data:
        df = pd.DataFrame(csv_data).convert_dtypes()
        df[['Event Duration (Days)', 'Period (Starting Date)']] = df['Date'].map(parse_date).apply(pd.Series)
        df['Period (Starting Date)'] = pd.to_datetime(df['Period (Starting Date)'], format='%d/%b/%Y')
        df['Data From'] = df['Period (Starting Date)'].min()
        df['Data To'] = df['Period (Starting Date)'].max()
        df['Data Source'] = 'Insurance Council of New Zealand'
        df['Last Updated'] = date.today()
        df['Area'] = 'New Zealand'
        
        columns_order = ['Area', 'Event', 'Categories', 'Period (Starting Date)', 'Event Duration (Days)',
                         'Value (Cost, $m)', 'Value (Inflation Adjusted Cost, $m)', 'Data From', 'Data To',
                         'Data Source', 'Last Updated']
        
        cleaned_df = df[columns_order]
        logging.info("Data cleaning completed successfully")
        save_to_csv(cleaned_df, pipeline_folder, filename)

def save_to_csv(data, folder, base_filename):
    try:
        if data is None:
            return
        s3_credentials = get_s3_credentials()
        s3 = boto3.resource(
            's3',
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key']
        ) 

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        filename = f"{folder}{base_filename}_{timestamp}.csv"
        csv_buffer = StringIO()

        data.to_csv(csv_buffer, index=False)
        s3.Object(download_folder, filename).put(Body=csv_buffer.getvalue())
        logging.info(f"Data saved successfully to {filename}")
        print(f"Data saved successfully! {filename}")
        upload_data_to_pcloud(data, filename)
    except Exception as e:
        logging.error(f"Failed to save data to CSV: {e}")

def upload_data_to_pcloud(data, filename):
    try:
        pcloud_token = get_pcloud_token()
        headers = {'Authorization': pcloud_token}
        files = {'file': (filename, pd.DataFrame(data).to_csv(index=False))}
        data = {'folderid': pcloud_folder_id}
        request_path = 'https://eapi.pcloud.com/uploadfile'

        response = requests.post(request_path, headers=headers, files=files, data=data)

        if response.status_code == 200:
            print("Data uploaded successfully to pCloud!")
        else:
            print(f"Failed to upload data to pCloud: {response.status_code}")
    except Exception as e:
        print(f"An error occurred during pCloud upload: {e}")

def main():
    url = 'https://www.icnz.org.nz/industry/cost-of-natural-disasters/'
    
    # Fetch HTML
    html_data = fetch_html(url)
    
    if html_data:
        # Extract Data
        extracted_data = extract_data(html_data)
        
        # Save Raw Data
        save_raw_data(extracted_data)
        
        # Clean Data and Save
        clean_dataframe(extracted_data)
        
        print("Data extraction and saving completed.")

if __name__ == "__main__":
    main()