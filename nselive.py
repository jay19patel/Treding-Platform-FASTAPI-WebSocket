import requests
import datetime
import http.client
import json
import pandas as pd


class NSEDataFetcher:
    def __init__(self):
        self.base_headers = {
            'Host': 'www.nseindia.com',
            'Referer': 'https://www.nseindia.com/get-quotes/equity?symbol=SBIN',
            'X-Requested-With': 'XMLHttpRequest',
            'pragma': 'no-cache',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
        }

    def fetch_data(self, url):
        try:
            response = requests.get(url, headers=self.base_headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data from {url}: {str(e)}")
        return None

    def fetch_nifty_option(self):
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        return self.fetch_data(url)

    def fetch_nifty_bank_option(self):
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY"
        return self.fetch_data(url)

    def fetch_nifty_stock_indices(self):
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        return self.fetch_data(url)

    def fetch_nifty_bank_stock_indices(self):
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK"
        return self.fetch_data(url)

    @staticmethod
    def convert_date_format(input_date):
        try:
            output_date_str = datetime.datetime.strptime(input_date, "%d-%b-%Y").strftime("%d-%m-%Y")
            return output_date_str
        except:
            return None

    def fetch_put_call_ratio(self, date):
        try:
            formatted_date = self.convert_date_format(date)
            url = f"https://api.indiainfoline.com/api/cmots/v1/derivatives/put-call-ratio/idx/all/{formatted_date}/Symbol/5/Desc"
            response = requests.get(url)
            pcr_data = response.json()
            PCRatioOI = float(pcr_data['data'][0]['PCRatioOI'])
            return PCRatioOI
        except:
            return None

    def fetch_vix_india(self):
        try:
            conn = http.client.HTTPSConnection("appfeeds.moneycontrol.com")
            payload = ''
            headers = {}
            conn.request("GET", "/jsonapi/market/indices&format=json&t_device=iphone&t_app=MC&t_version=48&ind_id=36", payload, headers)
            res = conn.getresponse()
            if res.status == 200:
                data = res.read()
                vix_alldata = json.loads(data.decode("utf-8"))
                vix = float(vix_alldata.get('indices', {}).get('lastprice', 0))
                return vix
            else:
                print(f"Failed to fetch VIX data. Status code: {res.status}")
        except Exception as e:
            print(f"Error fetching VIX data: {str(e)}")
        return None

 

    def nifty_chain(self):
        try:

            nifty_option_data = self.fetch_nifty_option()
            expiry_date = nifty_option_data['records'].get('expiryDates',None)
            nifty_current_price = nifty_option_data['records']['underlyingValue']
            nifty_chain_range = (nifty_current_price + 150, nifty_current_price - 150)
            df = pd.DataFrame(nifty_option_data['filtered']['data'])
            filtered_df = df[(df['strikePrice'] >= nifty_chain_range[1]) & (df['strikePrice'] <= nifty_chain_range[0])]
            filtered_df.loc[:, 'PE_lastPrice'] = filtered_df['PE'].apply(lambda x: x['lastPrice'])
            filtered_df.loc[:, 'PE_openInterest'] = filtered_df['PE'].apply(lambda x: x['openInterest'])
            filtered_df.loc[:, 'PE_totalTradedVolume'] = filtered_df['PE'].apply(lambda x: x['totalTradedVolume'])
            filtered_df.loc[:, 'PE_change'] = filtered_df['PE'].apply(lambda x: x['change'])
            filtered_df.loc[:, 'PE_changeinOpenInterest'] = filtered_df['PE'].apply(lambda x: x['changeinOpenInterest'])
            filtered_df.loc[:, 'PE_impliedVolatility'] = filtered_df['PE'].apply(lambda x: x['impliedVolatility'])

            filtered_df.loc[:, 'CE_lastPrice'] = filtered_df['CE'].apply(lambda x: x['lastPrice'])
            filtered_df.loc[:, 'CE_openInterest'] = filtered_df['CE'].apply(lambda x: x['openInterest'])
            filtered_df.loc[:, 'CE_totalTradedVolume'] = filtered_df['CE'].apply(lambda x: x['totalTradedVolume'])
            filtered_df.loc[:, 'CE_change'] = filtered_df['CE'].apply(lambda x: x['change'])
            filtered_df.loc[:, 'CE_changeinOpenInterest'] = filtered_df['CE'].apply(lambda x: x['changeinOpenInterest'])
            filtered_df.loc[:, 'CE_impliedVolatility'] = filtered_df['CE'].apply(lambda x: x['impliedVolatility'])

            filtered_df.loc[:, 'PCR'] = filtered_df['PE_openInterest'] / filtered_df['CE_openInterest']
            return  [row.drop('strikePrice').to_dict() | {'strikePrice': row['strikePrice']} for _, row in filtered_df.iterrows()]
        except:
            return None
    def niftybank_chain(self):
        try:
            
            nifty_option_data = self.fetch_nifty_bank_option()
            expiry_date = nifty_option_data['records'].get('expiryDates',None)
            nifty_current_price = nifty_option_data['records']['underlyingValue']
            nifty_chain_range = (nifty_current_price + 300, nifty_current_price - 300)
            df = pd.DataFrame(nifty_option_data['filtered']['data'])
            filtered_df = df[(df['strikePrice'] >= nifty_chain_range[1]) & (df['strikePrice'] <= nifty_chain_range[0])]
            filtered_df.loc[:, 'PE_lastPrice'] = filtered_df['PE'].apply(lambda x: x['lastPrice'])
            filtered_df.loc[:, 'PE_openInterest'] = filtered_df['PE'].apply(lambda x: x['openInterest'])
            filtered_df.loc[:, 'PE_totalTradedVolume'] = filtered_df['PE'].apply(lambda x: x['totalTradedVolume'])
            filtered_df.loc[:, 'PE_change'] = filtered_df['PE'].apply(lambda x: x['change'])
            filtered_df.loc[:, 'PE_changeinOpenInterest'] = filtered_df['PE'].apply(lambda x: x['changeinOpenInterest'])
            filtered_df.loc[:, 'PE_impliedVolatility'] = filtered_df['PE'].apply(lambda x: x['impliedVolatility'])

            filtered_df.loc[:, 'CE_lastPrice'] = filtered_df['CE'].apply(lambda x: x['lastPrice'])
            filtered_df.loc[:, 'CE_openInterest'] = filtered_df['CE'].apply(lambda x: x['openInterest'])
            filtered_df.loc[:, 'CE_totalTradedVolume'] = filtered_df['CE'].apply(lambda x: x['totalTradedVolume'])
            filtered_df.loc[:, 'CE_change'] = filtered_df['CE'].apply(lambda x: x['change'])
            filtered_df.loc[:, 'CE_changeinOpenInterest'] = filtered_df['CE'].apply(lambda x: x['changeinOpenInterest'])
            filtered_df.loc[:, 'CE_impliedVolatility'] = filtered_df['CE'].apply(lambda x: x['impliedVolatility'])

            filtered_df.loc[:, 'PCR'] = filtered_df['PE_openInterest'] / filtered_df['CE_openInterest']
            return [row.drop('strikePrice').to_dict() | {'strikePrice': row['strikePrice']} for _, row in filtered_df.iterrows()]
        except:
            return None

    def fetch_all_data(self):
        nifty_option_data = self.fetch_nifty_option()
        nifty_bank_option_data = self.fetch_nifty_bank_option()
        nifty_stock_indices_data = self.fetch_nifty_stock_indices()
        nifty_bank_stock_indices_data = self.fetch_nifty_bank_stock_indices()
        put_call_ratio_data = None

        if nifty_option_data != None:
            last_expiry_data = nifty_option_data.get('records', {}).get('expiryDates', [])[0]
            put_call_ratio_data = self.fetch_put_call_ratio(last_expiry_data)

        vix_data = self.fetch_vix_india()

        nifty_chain_data = self.nifty_chain()
        niftybank_chain_data = self.niftybank_chain()

        response_data = {
            'nifty_option_data': nifty_option_data,
            'nifty_bank_option_data': nifty_bank_option_data,
            'nifty_stock_indices_data': nifty_stock_indices_data,
            'nifty_bank_stock_indices_data': nifty_bank_stock_indices_data,
            'put_call_ratio_data': put_call_ratio_data,
            'vix_data': vix_data,
            'nifty_chain_data': nifty_chain_data,
            'niftybank_chain_data': niftybank_chain_data
        }

        return response_data

# # Example usage:
# if __name__ == "__main__":
#     nse_data_fetcher = NSEDataFetcher()
#     all_data = nse_data_fetcher.fetch_all_data()
#     print(all_data)
