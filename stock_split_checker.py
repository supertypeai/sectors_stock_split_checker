from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

import re 
import json
import os
import logging
import pandas as pd
import requests
import time 


load_dotenv()


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler('scrapper.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)

LOGGER.addHandler(file_handler)

LOGGER.info("Init Global Variable")


class StockSplitChecker:
    def __init__(self, supabase_client):
        self.urls = [
            'https://www.new.sahamidx.com/?/stock-split/page/1'
        ]
        self.supabase_client = supabase_client
        self.current_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        response = self.supabase_client.table("idx_stock_split").select("*").execute()
        data = pd.DataFrame(response.data)
        data = data.loc[data["date"] > self.current_date]
        data["split_ratio"] = data["split_ratio"].astype(float)
        self.db_records_future = data.to_dict("records")
        self.db_records_to_delete = []
        self.retrieved_records = []

    def get_stock_split_records(self):
        for url in self.urls:
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception("Error retrieving data from SahamIDX")

            soup = BeautifulSoup(response.text, "lxml")
            rows = soup.find_all("tr")
          
            for row in rows:
                name_cell = row.find("td", {"data-header": "Nama"})
                ratio_cell = row.find("td", {"data-header": "Ratio"})
                date_cell = row.find("td", {"data-header": "Ex Date"})
                cum_date_cell = row.find("td", {"data-header": "Cum Date"})
                recording_date_cell = row.find("td", {"data-header": "Recording Date"})

                if not (name_cell and ratio_cell and date_cell):
                    LOGGER.info(f'some data are null: {name_cell} {ratio_cell} {date_cell}')
                    continue
                
                # Clean Ex Date
                date_str = date_cell.text.strip()
                date = datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

                # Clean cum_date 
                cum_date_str = cum_date_cell.text.strip()
                cum_date = datetime.strptime(cum_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
                
                # Clean recording_date 
                recording_date_str = recording_date_cell.text.strip()
                recording_date = datetime.strptime(recording_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

                if date <= self.current_date:
                    print(f'Skipping {date}')
                    continue

                # Get Symbol 
                name_text = name_cell.text.strip()
                symbol_match = re.search(r'\((.*?)\)', name_text)

                if not symbol_match:
                    LOGGER.info(f'symbol not extracted: {name_cell} {date_cell}')
                    continue 

                symbol = symbol_match.group(1).strip() + ".JK"

                # Get Split Ratio
                ratio_str = ratio_cell.text.strip() 
                try:
                    parts = ratio_str.split(":")

                    if len(parts) != 2:
                        continue
                        
                    old_value = float(parts[0].strip())
                    new_value = float(parts[1].strip())
                    
                    if old_value == 0:
                        continue 
                        
                    split_ratio = new_value / old_value

                except (ValueError, TypeError):
                    continue
                
                data_dict = {
                    "symbol": symbol,
                    "date": date,
                    "split_ratio": round(split_ratio, 5),
                    "cum_date": cum_date, 
                    "recording_date": recording_date
                }
                self.retrieved_records.append(data_dict)

            time.sleep(2)

        LOGGER.info(f'Extracted data: {json.dumps(self.retrieved_records, indent=2)}')
        LOGGER.info(f'Extracted {len(self.retrieved_records)} data')

        for record in self.db_records_future:
            if record not in self.retrieved_records:
                self.db_records_to_delete.append(record)

        for record in self.retrieved_records:
            if record in self.db_records_future:
                self.retrieved_records.remove(record)

    def upsert_to_db(self):
        if self.db_records_to_delete:
            LOGGER.info("Deleting records due to update in source")
            for record in self.db_records_to_delete:
                try:
                    self.supabase_client.rpc(
                        "delete_stock_split_records",
                        params={
                            "symbol": record["symbol"],
                            "date": record["date"],
                            "split_ratio": record["split_ratio"],
                            "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
                        },
                    ).execute()
                    LOGGER.info(f"Successfully deleted record: {record}")
                except Exception as e:
                    LOGGER.error(f"Fail to delete record: {record}. Error: {e}")

        if not self.retrieved_records:
            LOGGER.warning("No records to upsert to database. All data is up to date")
            raise SystemExit(0)

        try:
            self.supabase_client.table("idx_stock_split").upsert(
                self.retrieved_records
            ).execute()
            LOGGER.info(
                f"Successfully upserted {len(self.retrieved_records)} data to database"
            )
            # Insert news
            LOGGER.info("Sending data to external endpoint")
            api_key = os.getenv("API_KEY")
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            response = requests.post(
                "https://sectors-news-endpoint.fly.dev/stock-split",
                headers=headers,
                data=json.dumps(self.retrieved_records)
            )
            if response.status_code == 200:
                LOGGER.info("Successfully sent data to external endpoint")
            else:
                LOGGER.info(f"Failed to send data to external endpoint. Status code: {response.status_code}")
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")


if __name__ == "__main__":
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    stock_split_checker = StockSplitChecker(supabase_client)
    stock_split_checker.get_stock_split_records()
    stock_split_checker.upsert_to_db()
    
    logging.info("Finish update stock split data")