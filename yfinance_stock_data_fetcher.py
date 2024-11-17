import yfinance as yf
import numpy as np
import pandas as pd
from baseclass_stock_data_fetcher import BaseclassStockDataFetcher

class YfinanceStockDataFetcher(BaseclassStockDataFetcher):
    def __init__(self, write_data=0, data_sub_folder_name =''):
        super().__init__(write_data, data_sub_folder_name,data_source_name='yfin')

    def fetch_data_and_format(self, symbol, **kwargs ): #i_ticker,'20000m','15m'
        df = yf.download(tickers=symbol, period=kwargs['period'], interval=kwargs['interval'])
        df.columns = df.columns.get_level_values(0)
        # print(df.columns)
        # df.to_csv('temp.csv')
        # exit(0)

        print("raw data columns fetched for stock : ", df.columns)
        print("shape of data fetched : ", df.shape)

        df = df.sort_index(ascending=True)
        df['Date'] = df.index
        df = df.reset_index(drop=True)
        # if "Datetime" in self.data_stock.columns:
        #    self.data_stock['Date'] = self.data_stock['Datetime']

        df['ser_no'] = np.arange(0, len(df))

        return df

    def fetch_batch_wise_full_data_with_given_interval(self, symbol, no_days_to_fetch, arg_interval):
        from datetime import date, timedelta
        # Specify start dates and end dates
        end_date = date.today()
        start_date = end_date - timedelta(days=no_days_to_fetch)
        print('start_date:', start_date)
        print('end_date:', end_date)
        start_date_list = [start_date + timedelta(days=5 * i) for i in range( int(np.round(no_days_to_fetch/5)) )]
        print('start_date_list:')
        #print(start_date_list)
        end_date_list = [start_date_list[i + 1] - timedelta(days=1) for i in range(int(np.round(no_days_to_fetch/5)) - 1)] + [end_date]
        print('end_date_list:')
        #print(end_date_list)
        print('===========================================================')
        # Use for-loop to extract data
        df_fetch = pd.DataFrame({})
        for idx in range(len(start_date_list)):
            data = yf.download(symbol,
                               start=start_date_list[idx].strftime("%Y-%m-%d"),
                               end=end_date_list[idx].strftime("%Y-%m-%d"),
                               interval=arg_interval)
            print('start date : ', start_date_list[idx].strftime("%Y-%m-%d"))
            print('end date : ', end_date_list[idx].strftime("%Y-%m-%d"))
            print("shape of data in batch : ",data.shape)
            df_fetch = df_fetch.append(data)


        df_fetch = df_fetch.sort_index(ascending=True)
        df_fetch['Date'] = df_fetch.index
        df = df_fetch.reset_index(drop=True)
        # if "Datetime" in self.data_stock.columns:
        #    self.data_stock['Date'] = self.data_stock['Datetime']

        df_fetch['ser_no'] = np.arange(0, len(df))

        return df_fetch

if __name__== '__main__':
    # out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name ='')\
    #     .run_fetcher('AXISBANK', period='20000m', interval='15m')
    # out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name ='')\
    #     .run_fetcher('ASIANPAINT.NS', period='20000m', interval='15m')
    #
    # print('shape of final output : ', out_df.shape)
    # #df = yf.download(tickers="HDFC", period = "1y", interval = "1h")
    # df = yf.download(tickers="HDFC.NS")
    # print(df.shape)
    #
    # out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
    #     .run_fetcher('ASIANPAINT.NS', period='2000d', interval='1d')

    # out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
    #     .run_fetcher('ASIANPAINT.NS', period='7d', interval='1m')
    # out_df.to_csv(r'temp\1m_data_check.csv')

    # out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
    #     .run_fetcher('^NSEBANK', period='7d', interval='1m')
    out_df = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
        .run_fetcher('^NSEBANK', period='1y', interval='1h')
    out_df.to_csv(r'temp\1m_data_check.csv')

    print('shape of final output : ', out_df.shape)