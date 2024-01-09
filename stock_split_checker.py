import os
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


class StockSplitChecker:
    def __init__(self, supabase_client):
        self.urls = [
            "https://sahamidx.com/?view=Stock.Split&path=Stock&field_sort=split_date&sort_by=DESC&page=1",
            "https://sahamidx.com/?view=Stock.Reverse&path=Stock&field_sort=reverse_date&sort_by=DESC&page=1",
        ]
        self.supabase_client = supabase_client
        self.current_date = datetime.today().strftime("%Y-%m-%d")
        response = self.supabase_client.table("idx_stock_split").select("*").execute()
        data = pd.DataFrame(response.data)
        data = data.loc[data["date"] > self.current_date]
        data["split_ratio"] = data["split_ratio"].astype(float)
        self.current_records = data.to_dict("records")
        self.records_to_delete = []
        self.records = []

    def get_stock_split_records(self):
        for url in self.urls:
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception("Error retrieving data from SahamIDX")

            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", {"class": "tbl_border_gray"})
            rows = table.find_all("tr", recursive=False)[1:]
            for row in rows:
                if len(row.find_all("td")) > 2:
                    values = row.find_all("td")
                    date = datetime.strptime(
                        values[-2].text.strip(), "%d-%b-%Y"
                    ).strftime("%Y-%m-%d")
                    if date <= self.current_date:
                        continue
                    old_value = float(values[3].text.strip().replace(",", ""))
                    new_value = float(values[4].text.strip().replace(",", ""))
                    split_ratio = new_value / old_value
                    data_dict = {
                        "symbol": values[1].find("a").text.strip() + ".JK",
                        "date": date,
                        "split_ratio": round(split_ratio, 5),
                    }
                    self.records.append(data_dict)

        for record in self.current_records:
            if record not in self.records:
                self.records_to_delete.append(record)

        for record in self.records:
            if record in self.current_records:
                self.records.remove(record)

    def upsert_to_db(self):
        if self.records_to_delete:
            print("Deleting records due to update in source")
            for record in self.records_to_delete:
                try:
                    self.supabase_client.rpc(
                        "delete_stock_split_records",
                        params={
                            "symbol": record["symbol"],
                            "date": record["date"],
                            "split_ratio": record["split_ratio"],
                        },
                    ).execute()
                    print(f"Successfully deleted record: {record}")
                except Exception as e:
                    print(f"Fail to delete record: {record}. Error: {e}")

        if not self.records:
            print("No records to upsert to database. All data is up to date")
            raise SystemExit(0)

        try:
            self.supabase_client.table("idx_stock_split").upsert(self.records).execute()
            print(f"Successfully upserted {len(self.records)} data to database")
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")


if __name__ == "__main__":
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    stock_split_checker = StockSplitChecker(supabase_client)
    stock_split_checker.get_stock_split_records()
    stock_split_checker.upsert_to_db()
