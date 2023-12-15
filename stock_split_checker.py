import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

class StockSplitChecker:
    def __init__(self, supabase_client) :
        self.urls = ["https://sahamidx.com/?view=Stock.Split&path=Stock", "https://sahamidx.com/?view=Stock.Reverse&path=Stock"]
        self.supabase_client = supabase_client
        self.records = []   
        self.current_date = datetime.today().strftime('%Y-%m-%d')
        response = self.supabase_client.table('idx_future_stock_split').select('*').execute()
        self.current_records = response.data
    
    def get_stock_split_records(self) :
        for url in self.urls:
            response = requests.get(url)
            if response.status_code != 200 :
                raise Exception("Error retrieving data from SahamIDX")
            
            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find('table', {'class':'tbl_border_gray'})
            rows = table.find_all('tr', recursive=False)[1:]
            for row in rows :
                if len(row.find_all('td'))>2:
                    values = row.find_all('td')
                    date = datetime.strptime(values[-2].text.strip(), '%d-%b-%Y').strftime('%Y-%m-%d')
                    if date<=self.current_date:
                        break
                    old_value = float(values[3].text.strip().replace(',',''))
                    new_value = float(values[4].text.strip().replace(',',''))
                    split_ratio = new_value / old_value
                    data_dict = {
                        'symbol':values[1].find('a').text.strip()+'.JK',
                        'date':date,
                        'split_ratio':round(split_ratio,5)
                    }
                    if data_dict not in self.current_records:
                        self.records.append(data_dict)

    def upsert_to_db(self):
        if not self.records:
            print("No records to upsert to database. All data is up to date")
            raise SystemExit(0)
        try:
            self.supabase_client.table('idx_future_stock_split').upsert(self.records).execute()
            print("Successfully upserted data to database")
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")
        
if __name__ == "__main__":
    url, key = os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY')
    supabase_client = create_client(url, key)
    
    stock_split_checker = StockSplitChecker(supabase_client)
    stock_split_checker.get_stock_split_records()
    stock_split_checker.upsert_to_db()
