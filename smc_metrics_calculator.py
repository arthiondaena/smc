import pandas
import pandas as pd
import numpy as np

class SmcMetricsCalculator():
    def __init__(self, stock_data, stock_name):
        temp = 0
        self.stock_data = stock_data
        self.stock_name = stock_name
        self.data_liq_low_data_df = pd.DataFrame(columns=['liq_category'])
        self.data_liq_high_data_df = pd.DataFrame(columns=['liq_category'])
        self.data_order_block_df = pd.DataFrame()
        self.data_super_data_supply_zones_df = pd.DataFrame(columns=['data_for_id','imb_open','imb_close','imb_diff'])
        self.data_super_data_demand_zones_df = pd.DataFrame(columns=['data_for_id','imb_open','imb_close'])
        self.data_imbalance_data = pd.DataFrame(columns=['imb_open', 'imb_close','del_imb'])
        self._price_values_to_select = ['Open', 'High', 'Low', 'Close']
        self._close_column_name = 'Close'
        self._open_column_name = 'Open'
        self._high_column_name = 'High'
        self._low_column_name = 'Low'
        self.id_column_name = 'ser_no'
        self.general_stop_loss_for_this_stock = 20	# we should have the provision to modify the stop loss dynamically based on stock price
        # self.position_pull_back_exit_target = self.general_stop_loss_for_this_stock * 1.5
        self.position_pull_back_exit_target = 15
        #create a new dataframe with name buy_order_dataframe
        self.buy_order_df = pd.DataFrame(columns=['trade_order_id','smc_zone_candle_no','previous_candle_no_from_which_smc_zones_are_fetched','trade_entry_candle_no','trade_entry_price','smc_zone_open_price','smc_zone_close_price','trade_status','expected_stop_loss','expected_target',
        'current_candle_no','current_candle_close_price','trade_exit_type','trade_exit_candle_no','trade_exit_price','trade_p_l',
        'target_1_price','target_2_price','target_3_price','target_4_price','target_1_hit_flag', 'target_2_hit_flag', 'target_3_hit_flag','target_4_hit_flag','max_target_reached_till_now','trade_iteration_price_delta'])

    
    def add_order_block_current_candle(self, arg_data,  id_col, current_id):
        #add current candle to the order block df
        sel_cols_list = self._price_values_to_select.copy()
        sel_cols_list.append(id_col)
        self.data_order_block_df = self.data_order_block_df._append(arg_data[arg_data[id_col]==current_id][sel_cols_list], ignore_index=True)
        return 0

    def add_liquidity_values_current_candle(self, arg_data, id_col, current_id):
        # if the current candle column named low is the lowest when compared to the last 5 candles then we can say that the current candle is a low line liquidity candle
        # if the current candle column named high is the highest when compared to the last 5 candles then we can say that the current candle is a high line liquidity candle
        
        #compare low value of current candle with the last 5 candles
        #compare high value of current candle with the last 5 candles
        sel_cols_list = self._price_values_to_select.copy()
        sel_cols_list.append(id_col)
        low_liq_swept, high_liq_swept = self.get_current_candle_liquidity_status(arg_data, id_col, current_id)
        if low_liq_swept == 1:
            self.data_liq_low_data_df = self.data_liq_low_data_df._append(arg_data[arg_data[id_col]==current_id][sel_cols_list], ignore_index=True)
            self.data_liq_low_data_df.loc[self.data_liq_low_data_df[id_col]==current_id,'liq_category'] = 'last_few_candles'

        if high_liq_swept == 1:
            self.data_liq_high_data_df = self.data_liq_high_data_df._append(arg_data[arg_data[id_col]==current_id][sel_cols_list], ignore_index=True)
            self.data_liq_high_data_df.loc[self.data_liq_high_data_df[id_col]==current_id,'liq_category'] = 'last_few_candles'

        return 0

    def get_current_candle_liquidity_status(self, arg_data, id_col, current_id,no_liq_candles=5):
        # compare column named low in current row in arg data with the last 5 rows relative to it in arg data
        flag_low_liq_swept = 1
        for i in np.arange(current_id-no_liq_candles, current_id, 1):
            if i in list(arg_data[id_col]):
                if list(arg_data[arg_data[id_col]==current_id][self._low_column_name])[0] > list(arg_data[arg_data[id_col]==i][self._low_column_name])[0]:
                    flag_low_liq_swept = 0


        # compare column named high in current row in arg data with the last 5 rows relative to it in arg data
        flag_high_liq_swept = 1
        for i in np.arange(current_id-no_liq_candles, current_id, 1):
            if i in list(arg_data[id_col]):
                if list(arg_data[arg_data[id_col]==current_id][self._high_column_name])[0] < list(arg_data[arg_data[id_col]==i][self._high_column_name])[0]:
                    flag_high_liq_swept = 0


        return  flag_low_liq_swept, flag_high_liq_swept
        


    def add_imbalance_values_current_candle(self, arg_data, id_col, current_id):
        # we are defining it as a situation where price has not touched the region in the last candle => need to think in future if we need to look for multiple candles
        # need to think whether we also need to define it as upside or downside liquidity=> as of now let us just define the liquidity zone
        # think whether we need the color of the candle for imbalance => positive imbalance could be demand and negative imbalance could be supply

        sel_cols_list = self._price_values_to_select.copy()
        sel_cols_list.append(id_col)
        print('list of cols : ',sel_cols_list)  
        # add the current candle into imbalance df
        self.data_imbalance_data = self.data_imbalance_data._append(arg_data[arg_data[id_col]==current_id][sel_cols_list], ignore_index=True)
        #['imb_open', 'imb_close']
        self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,'imb_open'] = list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,self._open_column_name])[0]
        self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,'imb_close'] = list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,self._close_column_name])[0]
        self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,'imb_high'] = list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,self._high_column_name])[0]
        self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,'imb_low'] = list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==current_id,self._low_column_name])[0]
        #run update_imbalance function for the current candle against previous candle
        if (current_id-1) in list(arg_data[id_col]):
            #self.update_imb_c1_wrt_to_c2(current_id, current_id-1)
            self.update_imbalance_of_current_candle_wrt_previous(current_id, current_id - 1)

        #loop on all the previous candles and update  the imbalance values against the current candle
        #for i in list(self.data_imbalance_data[id_col])[0:-1]:
		#    self.update_imb_c1_wrt_to_c2(i, current_id)

        return 0

    def update_imbalance_of_current_candle_wrt_previous(self, curr_candle, prev_candle): #update current wrt to previous candle, this happens for adding a new candle iteration
        temp = 0
        #for a green candle if close of current candle is greater than the previous candle high then there is some demand liquidity in this current candle
        #for a red candle if close of current candle is lower than the previous low then there exists some supply liquidity
        #in every other case there doesnt exist any demand or supply
        print(prev_candle)
        C1_open = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==curr_candle,'imb_open'])[0]
        C1_close =  list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==curr_candle,'imb_close'])[0]
        C2_high = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==prev_candle,'imb_high'])[0]
        C2_low = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==prev_candle,'imb_low'])[0]

        if (C1_open < C1_close) & (C1_close > C2_high):#for green candle liquidity exists
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_close']=C1_close
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_open']= max(C1_open, C2_high)
        elif ( C1_close < C1_open ) & (C1_close < C2_low):#for red candle liqiudity exists
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_close']=C1_close
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_open']= min(C1_open, C2_low)
        else:
            self.data_imbalance_data.loc[
                self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_close'] = 0
            self.data_imbalance_data.loc[
                self.data_imbalance_data[self.id_column_name] == curr_candle, 'imb_open'] = 0

        return 0

    def update_imb_c1_wrt_to_c2(self, c1_id, c2_id): #update this function to use for only comparing forward candle to update previous candle
        #C1_close > C2_low > C1_open  => condition for green candle => update imbalance values => imb_close=C2_low, imb_open=C1_open
        #C2_low > C1_close > C1_open  => condition for green candle => update imbalance values => imb_close=C1_close, imb_open=C1_open
        #C1_close > C2_high > C1_open  => condition for green candle => remove the complete imbalance => imb_high=0, imb_low=0
        #C1_open > C2_high > C1_close   => condition for red candle => update imbalance values => imb_close=C2_high, imb_open=C1_open
        #C1_open > C1_close > C2_high   => condition for red candle => update imbalance values => imb_close=C1_close, imb_open=C1_open
        #C1_open > C2_low > C1_close ` => condition for red candle => remove the complete imbalance => imb_high=0, imb_low=0
        C1_open = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==c1_id,'imb_open'])[0]
        C1_close =  list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==c1_id,'imb_close'])[0]
        C2_high = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==c2_id,'imb_high'])[0]
        C2_low = list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name]==c2_id,'imb_low'])[0]

        #if (C1_close > C2_low > C1_open)  then =C2_low, imb_open=C1_open
        # if (C2_low > C1_close > C1_open)  then imb_close=C1_close, imb_open=C1_open
        # if (C1_close > C2_high > C1_open)  then imb_close=0, imb_open=0
        if (C1_close > C2_low > C1_open): # this previous candle remains as a valid demand imbalance and imbalance needs to be updated
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_close']=C2_low
            #self.data_imbalance_data.loc[self.data_imbalance_data[id_col] == c1_id, 'imb_open']=C1_open
        elif (C2_low > C1_close > C1_open): # the entire candle values remains as a valid demand imbalance
            temp = 0
        elif (C1_close > C2_high > C1_open): #this means forward candle low is in the body and forward candle low is below the previous candle low => entire imbalance becomes invalid
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_close']=0
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_open']=0
        elif (C1_open > C2_high > C1_close): #for red candles below
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_close']=C2_high
            #self.data_imbalance_data.loc[self.data_imbalance_data[id_col] == c1_id, 'imb_open']=C1_open
        elif (C1_open > C1_close > C2_high):
            temp = 0
        elif (C1_open > C2_low > C1_close):
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_close']=0
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_open']=0
        else:
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_close']=0
            self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == c1_id, 'imb_open']=0


        return 0

    def create_demand_zone_for_this_candle(self, arg_data, id_col, zone_candle, current_id):
        self.data_super_data_demand_zones_df = self.data_super_data_demand_zones_df._append(
            self.data_order_block_df[self.data_order_block_df[id_col] == zone_candle])
        self.data_super_data_demand_zones_df.loc[
            (self.data_super_data_demand_zones_df[id_col] == zone_candle) & (self.data_super_data_demand_zones_df['data_for_id'].isna()), 'data_for_id'] = current_id

        self.data_super_data_demand_zones_df.loc[
            (self.data_super_data_demand_zones_df[id_col] == zone_candle) & (self.data_super_data_demand_zones_df['data_for_id']==current_id), 'imb_open'] = list(self.data_imbalance_data.loc[
            self.data_imbalance_data[id_col] == (zone_candle + 1), 'imb_open'])[0]
        self.data_super_data_demand_zones_df.loc[
            (self.data_super_data_demand_zones_df[id_col] == zone_candle) & (self.data_super_data_demand_zones_df['data_for_id']==current_id), 'imb_close'] = list(self.data_imbalance_data.loc[
            self.data_imbalance_data[id_col] == (zone_candle + 1), 'imb_close'])[0]
        self.data_super_data_demand_zones_df.loc[
            self.data_super_data_demand_zones_df[id_col] == zone_candle, 'imb_diff'] = \
        self.data_super_data_demand_zones_df.loc[
            self.data_super_data_demand_zones_df[id_col] == zone_candle, 'imb_close'] - \
        self.data_super_data_demand_zones_df.loc[
            self.data_super_data_demand_zones_df[id_col] == zone_candle, 'imb_open']

        return 0

    def create_supply_zone_for_this_candle(self, arg_data, id_col, zone_candle, current_id):
        self.data_super_data_supply_zones_df = self.data_super_data_supply_zones_df._append(
            self.data_order_block_df[self.data_order_block_df[id_col] == zone_candle])
        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'data_for_id'] = current_id

        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'imb_open'] = list(self.data_imbalance_data.loc[
            self.data_imbalance_data[id_col] == (zone_candle + 1), 'imb_open'])[0]
        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'imb_close'] = list(self.data_imbalance_data.loc[
            self.data_imbalance_data[id_col] == (zone_candle + 1), 'imb_close'])[0]
        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'imb_diff'] = \
        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'imb_close'] - \
        self.data_super_data_supply_zones_df.loc[
            self.data_super_data_supply_zones_df[id_col] == zone_candle, 'imb_open']

        return 0

    def update_imbalance_and_orderblock_all_previous_values(self, arg_data, id_col, current_id):
        #loop on all the previous candles and update  the imbalance values against the current candle
        for i in list(self.data_imbalance_data[id_col])[0:-1]:
            if list(self.data_imbalance_data.loc[self.data_imbalance_data[self.id_column_name] == i, 'del_imb'])[0]!=1:
                self.update_imb_c1_wrt_to_c2(i, current_id)
                #check if imbalance of i is 0
                if (list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==i,'imb_close'])[0] == 0):
                    #delete the value of i-1 in self.data_order_block_df
                    self.data_order_block_df = self.data_order_block_df[self.data_order_block_df[id_col] != i-1]

        #delete all rows from imbalance dataframe where imb_close = 0
        #self.data_imbalance_data = self.data_imbalance_data[self.data_imbalance_data['imb_close'] != 0]
        self.data_imbalance_data.loc[self.data_imbalance_data['imb_close'] == 0, 'del_imb'] = 1

        #select all the applicable demand and supply zones
        #create super dataframe which stores all the valid demand and supply zones for each of the previous candles
        #for all the valid imbalances check if there is liquidity sweep in the last 2 candles then the previous order block will become a either supply or demand zones
        #imbalance exists and there is no liquidity sweep in the last 2 candles then consider previous order block with some more conditions which we will think about in the further updates
        #list_of_ids_with_valid_imbalance = list(self.data_imbalance_data[id_col])
        ignore_liq=0

        list_of_ids_with_valid_imbalance = list(self.data_imbalance_data[self.data_imbalance_data['del_imb'] != 1][id_col])
        for i in list_of_ids_with_valid_imbalance:
            #check if close is greater than open in the imbalance dataframe for this id
            if (list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==i,'imb_close'])[0] > list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==i,'imb_open'])[0] ):
                #check if low line liquidity exists in the last 2 candles
                if (ignore_liq==1) or (i-1) in list(self.data_liq_low_data_df[id_col]):
                    self.create_demand_zone_for_this_candle(arg_data, id_col, i-1, current_id)
                elif i-1 not in list_of_ids_with_valid_imbalance:
                    if (i-2) in list(self.data_liq_low_data_df[id_col]):
                        self.create_demand_zone_for_this_candle(arg_data, id_col, i-1, current_id)

            if ( list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==i,'imb_close'])[0] < list(self.data_imbalance_data.loc[self.data_imbalance_data[id_col]==i,'imb_open'])[0]):
                #check if high line liquidity exists in the last 2 candles
                if (ignore_liq==1) or (i-1) in list(self.data_liq_high_data_df[id_col]):
                    #liquidity exists for this candle, this should be a good demand zone
                    self.create_supply_zone_for_this_candle(arg_data, id_col, i-1, current_id)
                elif (i-1) not in list_of_ids_with_valid_imbalance:
                    if (i-2) in list(self.data_liq_high_data_df[id_col]):
                        self.create_supply_zone_for_this_candle(arg_data, id_col, i-1, current_id)


        return 0

    def get_trade_order_id(self):
        #if length buy dataframe is 0 then return 1 else return max of the trade_order_id + 1 in from the buy order DataFrame
        if self.buy_order_df.shape[0] == 0:
            return 1
        else:
            return self.buy_order_df['trade_order_id'].max() + 1

    def add_targets_to_the_buy_order_df_for_current_candle(self, trade_order_id_current):
        #create targets for the current trade id based on below logic
        #create 4 targets for the current trade id
        #target 1 - 1:1
        #target 2 - 1:2
        #target 3 - 1:4
        #target 4 - 1:6
        #add these targets to the buy order dataframe
        base_target = self.general_stop_loss_for_this_stock

        #create target 1
        #update target_1_price value in  the buy order dataframe with the base target value
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_1_price'] = base_target
        #update target_1_status value in  the buy order dataframe with the value 0
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_1_hit_flag'] = 0
        # update target_2_price value in  the buy order dataframe with the base target value * 2
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_2_price'] = base_target * 2
        # update target_2_status value in  the buy order dataframe with the value 0
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_2_hit_flag'] = 0
        # update target_3_price value in  the buy order dataframe with the base target value * 4
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_3_price'] = base_target * 4
        # update target_3_status value in  the buy order dataframe with the value 0
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_3_hit_flag'] = 0
        # update target_4_price value in  the buy order dataframe with the base target value * 6
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_4_price'] = base_target * 6
        # update target_4_status value in  the buy order dataframe with the value 0
        self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == trade_order_id_current, 'target_4_hit_flag'] = 0

        return 0

    def check_if_buy_entry_exist_for_current_candle(self, current_id):
        previous_candle = current_id - 1
        trade_order_id_for_current_candle = self.get_trade_order_id()

        #get all the demand zones from the super dataframe for the previous candle
        valid_demand_zones = self.data_super_data_demand_zones_df[self.data_super_data_demand_zones_df['data_for_id'] == previous_candle]
        #select the row from valid_demand_zones which is the closest to the current candle meaning which has the maximum ser_no
        if valid_demand_zones.shape[0] > 0:
            closest_demand_zone = valid_demand_zones[valid_demand_zones['ser_no'] == valid_demand_zones['ser_no'].max()]
            #check if the current candle close is within the closest demand zone
            if (self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0] >= closest_demand_zone['Low'].values[0]) and (self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0] < closest_demand_zone['High'].values[0]):
                #take a buy trade here
                dict_data = {'smc_zone_candle_no' : closest_demand_zone['ser_no'].values[0],'previous_candle_no_from_which_smc_zones_are_fetched': previous_candle,'trade_entry_candle_no': current_id,
                'trade_entry_price' : self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0],
                'smc_zone_open_price': min(closest_demand_zone['High'].values[0], closest_demand_zone['Low'].values[0]),'smc_zone_close_price': max(closest_demand_zone['High'].values[0], closest_demand_zone['Low'].values[0]),'trade_status':'Entered'}

                #append above dictionary to buy order  dataframe
                self.buy_order_df = self.buy_order_df._append(dict_data, ignore_index=True)

        #get the trade order id from the get_trade_order function and equate it to the column in buy order dataframe for current candle
        self.buy_order_df.loc[self.buy_order_df['trade_entry_candle_no'] == current_id, 'trade_order_id'] = trade_order_id_for_current_candle

        #in buy dataframe, check if there is any null value in stop loss column and if yes then update stop loss to imb_close - imb open
        if self.buy_order_df['expected_stop_loss'].isnull().values.any():
            self.buy_order_df.loc[self.buy_order_df['expected_stop_loss'].isnull(), 'expected_stop_loss'] = self.buy_order_df['smc_zone_open_price'] - self.buy_order_df['smc_zone_close_price']

        #'expected_stop_loss','expected_target',
        #in buy dataframe, check if there is any null value in target column and if yes then update the target to 2 times the stop loss
        if self.buy_order_df['expected_target'].isnull().values.any():
            self.buy_order_df.loc[self.buy_order_df['expected_target'].isnull(), 'expected_target'] = -1 * self.buy_order_df['expected_stop_loss'] * 2
        

        self.add_targets_to_the_buy_order_df_for_current_candle(trade_order_id_for_current_candle)
        return 0

    def update_buy_orders_for_current_candle(self, current_id):
        #loop through all the buy orders where trade status=Entered
        for i in list(self.buy_order_df.loc[self.buy_order_df['trade_status']=='Entered','trade_order_id']):
            self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'] = self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0] - self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_entry_price'].values[0]

            #create stop loss exit condition for the buy order when close price of current candle price is less than the zone opening price
            if (self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0] < self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'smc_zone_open_price'].values[0]) or ((-1*self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0]) > self.general_stop_loss_for_this_stock):
                #update trade status to stop loss hit
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_status'] = 'Exited'
                #update trade exit price to candle closing price
                #self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_price'] = self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0]
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_price'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_entry_price'] - self.general_stop_loss_for_this_stock
                #update trade exit candle no to the current candle no
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_candle_no'] = current_id
                #update trade exit type to stop loss
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_type'] = 'Stop Loss'
                #update the p_l column in the buy order dataframe for this trade order id to trade exit price - trade entry price
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_p_l'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_price'] - self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_entry_price']



            #max_target_reached_till_now
            #now create target based exits on condition when we reach the respective targets
            #first update the specifc target hit variable based on the price
            #check if close price is greater than the target_1_price and target_1_flag=0, if so then update the target_1_flag to 1 based on the trade order id
            #calcualte a trade_iteration_p_l column where it is equal to trade_entry_price - current interation close price


            if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0] > self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_1_price'].values[0] and self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_1_hit_flag'].values[0] == 0:
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_1_hit_flag'] = 1
                #update max_target_reached to target_1_price
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_1_price']

            #check if close price is greater than the target_2_price and target_2_flag=0, if so then update the target_2_flag to 1 based on the trade order id
            if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0] > self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_2_price'].values[0] and self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_2_hit_flag'].values[0] == 0:
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_2_hit_flag'] = 1
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_2_price']

            #check if close price is greater than the target_3_price and target_3_flag=0, if so then update the target_3_flag to 1 based on the trade order id
            if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0] > self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_3_price'].values[0] and self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_3_hit_flag'].values[0] == 0:
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_3_hit_flag'] = 1
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_3_price']

            #check if close price is greater than the target_3_price and target_3_flag=0, if so then update the target_3_flag to 1 based on the trade order id
            if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0] > self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_4_price'].values[0] and self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_4_hit_flag'].values[0] == 0:
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_4_hit_flag'] = 1
                self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'target_4_price']


            #check if the close price less than max_target_reached_till_now by position_pull_back_exit_target, if so then exit the trade by considering the target has been hit
            #create an if condition which calculates that max target reached is not null fot this trade id
            if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'].values[0] is not None:
                if self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_iteration_price_delta'].values[0] < (self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'max_target_reached_till_now'].values[0] - self.position_pull_back_exit_target):
                    #update trade exit price to candle closing price and similarly update trade exit related columns
                    self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_status'] = 'Exited'
                    self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_candle_no'] = current_id
                    #update trade exit type to target hit
                    self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_type'] = 'Target Hit'
                    self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_price'] = self.stock_data.loc[self.stock_data[self.id_column_name] == current_id, 'Close'].values[0]
                    self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_p_l'] = self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_exit_price'] - self.buy_order_df.loc[self.buy_order_df['trade_order_id'] == i, 'trade_entry_price']

        return 0

    def check_if_entry_exist_for_current_candle(self, current_id):
        self.check_if_buy_entry_exist_for_current_candle(current_id)

        return 0

    def update_all_the_active_trades_for_current_candle(self, current_id):
        self.update_buy_orders_for_current_candle(current_id)
        return 0
    
            
    def calc_smc_metrics(self):
        for i in list(self.stock_data['ser_no']):
            print('iteration candle ', i)
            self.add_liquidity_values_current_candle(self.stock_data, 'ser_no', i)
            self.add_order_block_current_candle(self.stock_data,  'ser_no', i)
            self.add_imbalance_values_current_candle(self.stock_data, 'ser_no', i)
            self.update_imbalance_and_orderblock_all_previous_values(self.stock_data, 'ser_no', i)
            self.check_if_buy_entry_exist_for_current_candle(i)
            self.update_all_the_active_trades_for_current_candle(i)
        # i = self.stock_data['ser_no'].iloc[-1] - 10
        # print('iteration candle ', i)
        # self.add_liquidity_values_current_candle(self.stock_data, 'ser_no', i)
        # self.add_order_block_current_candle(self.stock_data,  'ser_no', i)
        # self.add_imbalance_values_current_candle(self.stock_data, 'ser_no', i)
        # self.update_imbalance_and_orderblock_all_previous_values(self.stock_data, 'ser_no', i)
        # self.check_if_buy_entry_exist_for_current_candle(i)
        # self.update_all_the_active_trades_for_current_candle(i)  


        demand_file = r'intermediate_data/smc_zones_output1/1m_yfin' + "//" + self.stock_name + "_demand_zone.csv"
        supply_file = r'intermediate_data/smc_zones_output1/1m_yfin' + "//" + self.stock_name + "_supply_zone.csv"
        imbalance_file = r'intermediate_data/smc_zones_output1/1m_yfin' + "//" + self.stock_name + "_imbalance_output.csv"
        liq_file = r'intermediate_data/smc_zones_output1/1m_yfin' + "//" + self.stock_name + "_liq_output.csv"
        orderbook_file = r'intermediate_data/smc_zones_output1/1m_yfin' + "//" + self.stock_name + "_orderbook_output.csv"
        buy_orders_dataframe = r'intermediate_data/smc_zones_output1/1m_yfin/buy_orders' + "//" + self.stock_name + "_buy_orders_output.csv"
        self.data_super_data_demand_zones_df.to_csv(demand_file,index=False)
        self.data_super_data_supply_zones_df.to_csv(supply_file,index=False)
        self.data_imbalance_data.to_csv(imbalance_file,index=False)
        self.data_liq_low_data_df.to_csv(liq_file, index=False)
        self.data_order_block_df.to_csv(orderbook_file, index=False)
        self.buy_order_df.to_csv(buy_orders_dataframe, index=False)
	



if __name__ == "__main__":
    from yfinance_stock_data_fetcher import YfinanceStockDataFetcher
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\	'
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\HDFCBANK_chart_patterns.csv'
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\HDFCLIFE_chart_patterns.csv'
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\HEROMOTOCO_chart_patterns.csv'
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\HINDALCO_chart_patterns.csv'
    # inp_path_with_file = r'C:\Users\mdevaray\Documents\self\stock_market_project\Git_repos\stock_market_analyser\working_data\chart_patterns\yfin\HINDUNILVR_chart_patterns.csv'

    list_of_data_sources = ['HDFC_chart_patterns', 'HDFCBANK_chart_patterns', 'HDFCLIFE_chart_patterns', 'HEROMOTOCO_chart_patterns', 'HINDALCO_chart_patterns', 'HINDUNILVR_chart_patterns']
    #list_of_data_sources = ['HINDUNILVR_chart_patterns']
    list_of_data_sources = ['RELIANCE']
    list_of_data_sources = ['CIPLA', 'BAJFINANCE', 'TATACONSUM', 'SHRIRAMFIN',
                            'TITAN', 'WIPRO', 'RELIANCE', 'TATASTEEL', 'ITC', 'HINDALCO']

    for i_file_name in list_of_data_sources:
        imp_file_name = r"/home/invicto/coding/algotrading/smc_code/intermediate_data/raw_data/yfin/" + i_file_name + ".csv"

        #inp_data = pd.read_csv(imp_file_name)

        if i_file_name in ['NSEI']:
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher('^NSEI', period='5d', interval='15m')
        elif i_file_name in ['NSEBANK']:
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher('^NSEBANK', period='5d', interval='15m')
        else:
            stock_name = i_file_name + '.NS'
            inp_data = YfinanceStockDataFetcher(write_data=1, data_sub_folder_name='') \
                .run_fetcher(stock_name, period='1mo', interval='15m')
        #out_df.to_csv(r'temp\1m_data_check.csv')

        inp_data =inp_data.reset_index(drop=True)
        print('shape of inp file : ', inp_data.shape)
        print('columns in inp file : ', inp_data.columns)

        updated_ser_no_data = inp_data[-500:].copy()
        #updated_ser_no_data['ser_no'] = updated_ser_no_data['ser_no'] - updated_ser_no_data['ser_no'].min()
        #updated_ser_no_data = inp_data

        out_data = SmcMetricsCalculator(updated_ser_no_data, i_file_name).calc_smc_metrics()


        #out_data.to_csv(out_path_with_file, index=False)
        #print('shape of out file : ', out_data.shape)
        # out_data.to_csv('temp.csv')

        print('finished : ', i_file_name)
