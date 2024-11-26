import pandas as pd
import numpy as np

def fvg(data):
    """
    FVG - Fair Value Gap
    A fair value gap is when the previous high is lower than the next low if the current candle is bullish.
    Or when the previous low is higher than the next high if the current candle is bearish.

    parameters:
    
    returns:
    FVG = 1 if bullish fair value gap, -1 if bearish fair value gap
    Top = the top of the fair value gap
    Bottom = the bottom of the fair value gap
    MitigatedIndex = the index of the candle that mitigated the fair value gap
    """

    fvg = np.where(
        (
            (data["High"].shift(1) < data["Low"].shift(-1))
            & (data["Close"] > data["Open"])
        )
        | (
            (data["Low"].shift(1) > data["High"].shift(-1))
            & (data["Close"] < data["Open"])
        ),
        np.where(data["Close"] > data["Open"], 1, -1),
        np.nan,
    )

    top = np.where(
        ~np.isnan(fvg),
        np.where(
            data["Close"] > data["Open"],
            data["Low"].shift(-1),
            data["Low"].shift(1),
        ),
        np.nan,
    )

    bottom = np.where(
        ~np.isnan(fvg),
        np.where(
            data["Close"] > data["Open"],
            data["High"].shift(1),
            data["High"].shift(-1),
        ),
        np.nan,
    )

    mitigated_index = np.zeros(len(data), dtype=np.int32)
    for i in np.where(~np.isnan(fvg))[0]:
        mask = np.zeros(len(data), dtype=np.bool_)
        if fvg[i] == 1:
            mask = data["Low"][i + 2 :] <= top[i]
        elif fvg[i] == -1:
            mask = data["High"][i + 2 :] >= bottom[i]
        if np.any(mask):
            j = np.argmax(mask) + i + 2
            mitigated_index[i] = j

    mitigated_index = np.where(np.isnan(fvg), np.nan, mitigated_index)

    return pd.concat(
        [
            pd.Series(fvg.flatten(), name="FVG"),
            pd.Series(top.flatten(), name="Top"),
            pd.Series(bottom.flatten(), name="Bottom"),
            pd.Series(mitigated_index.flatten(), name="MitigatedIndex"),
        ],
        axis=1,
    )

def order_block(data):

    ob = np.where(
        (
            (data["High"] < data["Low"].shift(-2))
            & (data["Close"] > data["Open"])
        )
        | (
            (data["Low"] > data["High"].shift(-2))
            & (data["Close"] < data["Open"])
        ),
        np.where(data["Close"] > data["Open"], 1, -1),
        np.nan,
    )
    # ob = ob.flatten()

    top = np.where(
        ~np.isnan(ob),
        np.where(
            data["Close"] > data["Open"],
            data["Low"].shift(-2),
            data["Low"],
        ),
        np.nan,
    )

    bottom = np.where(
        ~np.isnan(ob),
        np.where(
            data["Close"] > data["Open"],
            data["High"],
            data["High"].shift(-2),
        ),
        np.nan,
    )

    mitigated_index = np.zeros(len(data), dtype=np.int32)
    for i in np.where(~np.isnan(ob))[0]:
        mask = np.zeros(len(data), dtype=np.bool_)
        if ob[i] == 1:
            mask = data["Low"][i + 3 :] <= top[i]
        elif ob[i] == -1:
            mask = data["High"][i + 3 :] >= bottom[i]
        if np.any(mask):
            j = np.argmax(mask) + i + 3
            mitigated_index[i] = int(j)
    ob = ob.flatten()
    mitigated_index1 = np.where(np.isnan(ob), np.nan, mitigated_index)

    return pd.concat(
        [
            pd.Series(ob.flatten(), name="OB"),
            pd.Series(top.flatten(), name="Top"),
            pd.Series(bottom.flatten(), name="Bottom"),
            pd.Series(mitigated_index1.flatten(), name="MitigatedIndex"),
        ],
        axis=1,
    )

def buy_signal(data):
    ob = order_block(data)
    ob.reset_index(drop=True)
    curr_index = len(data)-1
    mitigated_index = np.where(ob["OB"]==1, ob["MitigatedIndex"], np.nan)

    if curr_index in mitigated_index:
        return [True, ob['Bottom'].iloc[np.where(mitigated_index == curr_index)[0]].values[-1]]
    else:
        return [False, 0]

def add_buy(data, index, sl):
    dict_data = {'candle_no': index, 'entry': data['Close'].iloc[index], 'entry_date': data['Date'].iloc[index], 'sl': sl, 'target_price': data['Close'].iloc[index]+(data['Close'].iloc[index]-sl),
                 'sl_size': data['Close'].iloc[index]-sl, 'trade_status': 'Entered'}
    
    return dict_data

def test_ob(data, name='temp'):
    buy_order_df = pd.DataFrame(columns=['candle_no', 'entry', 'entry_date', 'sl', 'target_price', 'sl_size', 'P/L', 'trade_status'])

    for i in range(10, len(data)-1):
        temp_data = data[:i+1].copy()
        # temp_data.reset_index(drop=True)
        to_buy = buy_signal(temp_data)
        # print(to_buy)
        sl = to_buy[1]
        # sl = 20
        # print(f'truth {temp_data['Close'].iloc[i]}')
        temp_data.to_csv('temp.csv')
        if to_buy[0] and (sl < temp_data['Close'].iloc[i]):
            buy_order_df = buy_order_df._append(add_buy(temp_data, i, sl), ignore_index=True)

        for j in list(buy_order_df.loc[buy_order_df['trade_status']!='Closed', 'candle_no']):
            # print(data['Close'].iloc[-1])
            # print(list(buy_order_df.loc[buy_order_df['candle_no'] == i, 'sl'])[-1])

            if temp_data['Close'].iloc[-1] < list(buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'])[-1]:
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'trade_status'] = 'Closed'
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'P/L'] = buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'] - buy_order_df.loc[buy_order_df['candle_no'] == j, 'entry']
            if temp_data['Close'].iloc[-1] > list(buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price'])[-1]:
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'trade_status'] = 'Trailing'
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'] = buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price']
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price'] += buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl_size']
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'P/L'] = temp_data['Close'].iloc[-1] - buy_order_df.loc[buy_order_df['candle_no'] == j, 'entry']

    num_wins = len(buy_order_df[buy_order_df['P/L']>0]) if len(buy_order_df[buy_order_df['P/L']>0]) > 0 else 1
    num_loss = len(buy_order_df[buy_order_df['P/L']<0]) if len(buy_order_df[buy_order_df['P/L']<0]) > 0 else 1

    results = {'Stock_name': name, 'P/L': buy_order_df['P/L'].sum(), 'WL_ratio': num_wins/num_loss,
                'Avg_win': buy_order_df.loc[buy_order_df['P/L']>0, 'P/L'].sum()/num_wins,
                'Avg_loss': buy_order_df.loc[buy_order_df['P/L']<0, 'P/L'].sum()/num_loss}

    for k,v in results.items():
        if type(v) == float:
            results[k] = round(v, 2)

    buy_order_df.to_csv(f'new/ob/{name}_buy.csv')
    print("Total profit/loss = ", buy_order_df['P/L'].sum())
    print(f"Win Trades / Loss Trades = {len(buy_order_df[buy_order_df['P/L']>0])} / {len(buy_order_df[buy_order_df['P/L']<0])}")
    print(f"Average Profit per winning trade = {buy_order_df.loc[buy_order_df['P/L']>0, 'P/L'].sum()/num_wins}")
    print(f"Average Loss per lossing trade = {buy_order_df.loc[buy_order_df['P/L']<0, 'P/L'].sum()/num_loss}")

    return results    

def test_ema_ob(data, name = 'temp'):
    buy_order_df = pd.DataFrame(columns=['candle_no', 'entry', 'entry_date', 'sl', 'target_price', 'sl_size', 'P/L', 'trade_status'])
    data['EMA_12'] = data['Close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['Close'].ewm(span=26, adjust=False).mean()
    buy_flag = False
    count = 0
    sl = 0

    for i in range(10, len(data)-1):
        temp_data = data[:i+1].copy()
        to_buy = buy_signal(temp_data)
        if buy_flag == False and to_buy[0]:
            buy_flag = True
            sl = to_buy[1]
            count = 0
        if count>5:
            buy_flag = False
        count+=1
        if buy_flag:
            crossover = temp_data['EMA_12'].values[i] > temp_data['EMA_26'].values[i]

        if buy_flag and (sl < temp_data['Close'].iloc[i]) and crossover:
            buy_order_df = buy_order_df._append(add_buy(temp_data, i, sl), ignore_index=True)
            buy_flag = False

        for j in list(buy_order_df.loc[buy_order_df['trade_status']!='Closed', 'candle_no']):
            # print(data['Close'].iloc[-1])
            # print(list(buy_order_df.loc[buy_order_df['candle_no'] == i, 'sl'])[-1])

            if temp_data['Close'].iloc[-1] < list(buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'])[-1]:
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'trade_status'] = 'Closed'
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'P/L'] = buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'] - buy_order_df.loc[buy_order_df['candle_no'] == j, 'entry']
            if temp_data['Close'].iloc[-1] > list(buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price'])[-1]:
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'trade_status'] = 'Trailing'
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl'] = buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price']
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'target_price'] += buy_order_df.loc[buy_order_df['candle_no'] == j, 'sl_size']
                buy_order_df.loc[buy_order_df['candle_no'] == j, 'P/L'] = temp_data['Close'].iloc[-1] - buy_order_df.loc[buy_order_df['candle_no'] == j, 'entry']

    num_wins = len(buy_order_df[buy_order_df['P/L'] > 0]) if len(buy_order_df[buy_order_df['P/L'] > 0]) > 0 else 1
    num_loss = len(buy_order_df[buy_order_df['P/L'] < 0]) if len(buy_order_df[buy_order_df['P/L'] < 0]) > 0 else 1

    results = {'Stock_name': name, 'P/L': buy_order_df['P/L'].sum(), 'WL_ratio': num_wins / num_loss,
               'Avg_win': buy_order_df.loc[buy_order_df['P/L'] > 0, 'P/L'].sum() / num_wins,
               'Avg_loss': buy_order_df.loc[buy_order_df['P/L'] < 0, 'P/L'].sum() / num_loss}

    for k,v in results.items():
        if type(v) == float:
            results[k] = round(v, 2)

    buy_order_df.to_csv(f'new/ema_ob/{name}_buy.csv')
    print("Total profit/loss = ", buy_order_df['P/L'].sum())
    print(
        f"Win Trades / Loss Trades = {len(buy_order_df[buy_order_df['P/L'] > 0])} / {len(buy_order_df[buy_order_df['P/L'] < 0])}")
    print(f"Average Profit per winning trade = {buy_order_df.loc[buy_order_df['P/L'] > 0, 'P/L'].sum() / num_wins}")
    print(f"Average Loss per lossing trade = {buy_order_df.loc[buy_order_df['P/L'] < 0, 'P/L'].sum() / num_loss}")
    return results

if __name__ == "__main__":
    from yfinance_stock_data_fetcher import YfinanceStockDataFetcher

    # list_of_data_sources = ['NSEBANK']
    list_of_data_sources = ['CIPLA', 'BAJFINANCE', 'TATACONSUM', 'SHRIRAMFIN',
                            'TITAN', 'WIPRO', 'RELIANCE', 'TATASTEEL', 'ITC', 'HINDALCO']
    # list_of_data_sources = ['RELIANCE']
    
    results = pd.DataFrame()

    interval = '5m'

    for i_file_name in list_of_data_sources:
        if i_file_name in ['NSEI']:
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher('^NSEI', period='5d', interval=interval)
        elif i_file_name in ['NSEBANK']:
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher('^NSEBANK', period='1mo', interval=interval)
        else:
            stock_name = i_file_name + '.NS'
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher(stock_name, period='1mo', interval=interval)
        #out_df.to_csv(r'temp\1m_data_check.csv')
        # inp_data.to_csv('temp.csv')
        # exit(0)

        inp_data =inp_data.reset_index(drop=True)
        print('Stock : ', i_file_name)
        print('shape of inp file : ', inp_data.shape)

        # updated_ser_no_data = inp_data[-500:].copy()
        # #updated_ser_no_data['ser_no'] = updated_ser_no_data['ser_no'] - updated_ser_no_data['ser_no'].min()
        # #updated_ser_no_data = inp_data

        # out_data = SmcMetricsCalculator(updated_ser_no_data, i_file_name).calc_smc_metrics()
        # print(inp_data)
        # ob = order_block(inp_data)
        # ob.to_csv('ob.csv')
        # inp_data.to_csv('inp.csv')
        # print(ob[~np.isnan(ob['OB'])])
        # print(ob.shape)
        # exit(0)

        result = test_ob(inp_data, i_file_name)
        
        results = pd.concat([results, pd.DataFrame([result])], ignore_index=True)
        #out_data.to_csv(out_path_with_file, index=False)
        #print('shape of out file : ', out_data.shape)

        print(f'finished : {i_file_name}')
        print('------------------------------------------------------\n\n')
    
    results.to_csv(f'new/ob_{interval}.csv')
    print('Total P/L : ', results['P/L'].sum())