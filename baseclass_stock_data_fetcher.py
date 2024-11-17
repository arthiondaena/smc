import numpy as np


module_base_path = r'intermediate_data/raw_data'

class BaseclassStockDataFetcher():			
    def __init__(self, write_data=0, data_sub_folder_name ='',**kwargs):
        self.write_param = write_data
        self.data_source_name = kwargs['data_source_name']
        if data_sub_folder_name !='':
            self.module_output_folder = module_base_path + '//' + self.data_source_name + '//' + data_sub_folder_name
        else:
            self.module_output_folder = module_base_path + '//' + self.data_source_name

    def write_data(self, out_df, file_name):
        if self.write_param==1:
            file_with_path = self.module_output_folder + '//' + file_name + '_' + self.data_source_name + '.csv'
            out_df.to_csv(file_with_path, index=False)

        return 0

    def _add_1st_forward_day(self, arg_data, n, sel_col):
        dup_df = arg_data[['ser_no', sel_col]].copy()

        rename_sel_col = sel_col + "_p_" + str(n)
        dup_df = dup_df.rename(columns={sel_col: rename_sel_col})
        dup_df['ser_no'] = dup_df['ser_no'] - n

        arg_data = arg_data.merge(dup_df[['ser_no', rename_sel_col]], on='ser_no', how='left')
        arg_data[rename_sel_col] = np.where(arg_data[rename_sel_col].isna(), arg_data[sel_col],
                                                   arg_data[rename_sel_col])

        return arg_data

    def run_stock_split_sel_column(self, data, base_column, next_candle_col, list_of_additional_col ):
        data_size = len(data)
        data = data.reset_index(drop=True)
        data['stock_split_factor'] = 1

        data[next_candle_col] = np.where(data[next_candle_col].isna(), data[base_column], data[next_candle_col])
        for i_row in np.arange(data_size-1, -1, -1):
            if (data.loc[i_row,base_column]/data.loc[i_row, next_candle_col])>1.9 :
                data.loc[i_row, 'stock_split_factor'] = data.loc[i_row+1, 'stock_split_factor'] * np.round((data.loc[i_row,base_column]/data.loc[i_row, next_candle_col]))
            else:
                if (i_row+1) < data_size:
                    data.loc[i_row, 'stock_split_factor'] = data.loc[i_row+1, 'stock_split_factor']

        data[base_column] = data[base_column]/data['stock_split_factor']
        for i_col in list_of_additional_col:
            data[i_col] = data[i_col] / data['stock_split_factor']

        return data

    def run_fetcher(self,symbol, **kwargs):
        out_df = self.fetch_data_and_format(symbol, **kwargs)
        self.write_data(out_df, symbol)

        return out_df

    def fetch_data_and_format(self, symbol, **kwargs):
        raise NotImplementedError