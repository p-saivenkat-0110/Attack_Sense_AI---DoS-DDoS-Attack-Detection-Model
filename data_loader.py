import numpy as np
import pandas as pd
import os, joblib, json

class Utilities:
    """
    The `Utilities` class contains attributes used for data preprocessing and feature scaling.
    """
    def __init__(self):
        utilities_path = r".\Utilities\utilities.json"
        with open(utilities_path) as f:
            cfg = json.load(f)
        
        self.system_columns       = cfg["system_columns"] # `system_columns` contains features for System-level data.
        self.key_columns          = cfg["key_columns"] # `self.key_columns` attribute is used as key for aggregation of data to convert `Timestamp` from milliseconds resolution to seconds resolution.
        self.single_value_columns = cfg["single_value_columns"] # `self.single_value_columns` attribute is a list of column names that represent features with single values. These columns are excluded from the aggregation dictionary (`self.agg_dict`). 
        self.agg_dict             = cfg["agg_dict"] # `agg_dict` contains features for Network-level data with statistical method applied to each feature for aggreagtion of network-level data to convert `Timestamp` from milliseconds resolution to seconds resolution .
        for val in self.single_value_columns: 
            del self.agg_dict[val]
        self.columns_to_scale = list(self.agg_dict.keys())

class Normalization(Utilities):
    """
    `Normalization` class defines methods for preparing and applying min-max and z-score normalization on network and system-level features of a dataset.
    """
    def __init__(self):
        super().__init__()
        self.network_scaler = joblib.load(r".\Scalers\MinMaxScaler.save") # MinMaxScaler object
        self.system_scaler  = joblib.load(r".\Scalers\StandardScaler.save") # StandardScaler object
        
    def min_max_normalization(self, df):
        """
        Performs min-max normalization only on Network-level features.
        """
        _df = df.drop(self.key_columns, axis=1)
        scaled_df = self.network_scaler.transform(_df)
        scaled_df = pd.DataFrame(scaled_df, columns = self.columns_to_scale)
        df = pd.concat([df[self.key_columns], scaled_df], axis=1)
        return df

    def z_score_normalization(self,df):
        """
        Performs z-score normalization only on System-level features.
        """
        _df = df.drop(['Timestamp'], axis=1)
        scaled_df = self.system_scaler.transform(_df)
        scaled_df = pd.DataFrame(scaled_df, columns = self.system_columns)
        df = pd.concat([df['Timestamp'], scaled_df], axis=1)
        return df

class Data_Preprocessing(Normalization):
    def __init__(self):
        super().__init__()

    def __add_Timestamp(self,df):
        df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
        df = df.drop(["Date","Time"], axis=1)
        return df

    def __preprocess_NETWORK_DATA(self, df):
        # df = self.__add_Timestamp(df)
        ##### Aggregates NETWORK_DATA based on KEY COLUMNS ####
        df = df.drop(["Flow ID","Src IP","Dst IP"], axis=1)
        df = df.drop(self.single_value_columns,axis=1)
        df = df.groupby(self.key_columns).agg(self.agg_dict)

        ################## Handling Missing values & NULL values ##################
        df["Flow Bytes/s"] = df["Flow Bytes/s"].interpolate(method='linear')
        for col in ["Flow Bytes/s", "Flow Packets/s"]:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            max_value = (df[col].quantile(0.99))**10
            df[col] = df[col].fillna(max_value)
            df[col] = np.log1p(df[col])

        ############ MIN-MAX Normalization ############
        df = df.reset_index()
        df = self.min_max_normalization(df)

        return df

    def __preprocess_SYSTEM_DATA(self, df):
        # df = self.__add_Timestamp(df)
        ################## Handling Missing values & NULL values ##################
        for col in self.system_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].interpolate(method='linear')

        ############ Z-Score Normalization ############
        df = df.reset_index(drop=True)
        df = self.z_score_normalization(df)

        return df 
    
    def __merge_network_system(self, network, system, label):
        merged_df = pd.merge(network, system, on='Timestamp', how='left')
        merged_df = merged_df.dropna()
        merged_df = merged_df.reset_index(drop=True)
        merged_df["Label"] = label
        return merged_df

    def _preprocess(self, network_df, system_df, label=-1):
        network = self.__preprocess_NETWORK_DATA(network_df)
        system  = self.__preprocess_SYSTEM_DATA(system_df)
        merged_network_system = self.__merge_network_system(network,system,label)
        return merged_network_system

class LoadData(Data_Preprocessing):
    def __init__(self):
        super().__init__()
        self.__empty_df   = pd.DataFrame()
        self.__dataFolder     = os.path.join(os.getcwd(), r"NET_SYS")
        self.__network_csv    = os.path.join(self.__dataFolder, r"NETWORK\network_stream.csv")
        self.__system_csv     = os.path.join(self.__dataFolder, r"SYSTEM\system_stream.csv")
        self.__net_sys_stream = os.path.join(self.__dataFolder, "net_sys_stream.csv")

    def __load_data(self):
        ############ LOAD NETWORK DATA ############
        network_df  = self.__empty_df
        if os.path.isfile(self.__network_csv):
            network_df = pd.read_csv(self.__network_csv)
            network_df['Timestamp'] = pd.to_datetime(network_df['Timestamp'], format="%d/%m/%Y %I:%M:%S %p")
            network_df['Timestamp'] = network_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")

        ############ LOAD SYSTEM DATA ############
        system_df  = self.__empty_df
        if os.path.isfile(self.__system_csv):
            system_df = pd.read_csv(self.__system_csv)
            system_df.columns = ['Timestamp']+self.system_columns
            system_df['Timestamp'] = pd.to_datetime(system_df['Timestamp'], format="%m/%d/%Y %H:%M:%S.%f", errors='coerce')
            system_df['Timestamp'] = system_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")
        
        return (network_df, system_df)        
    
    def update_net_sys_stream(self):
        network_df, system_df = self.__load_data()
        updated_stream = self.__empty_df
        if not (network_df.empty or system_df.empty): 
            updated_stream = self._preprocess(network_df, system_df) 
    
        if not updated_stream.empty:
            updated_stream.to_csv(self.__net_sys_stream, index = False) 

    def fetch_past_K_minute_data(self, timestamp, window_size):
        try:
            df = pd.read_csv(self.__net_sys_stream, parse_dates=['Timestamp'])
            endTimestamp = timestamp.strftime('%Y-%m-%d %H:%M')
            df['group'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M')
            df = df.set_index('group')
            minutes = sorted(list(set(df.index)))
            endIndex = minutes.index(endTimestamp)+1
            startIndex = max(0, endIndex-window_size)
            windowed_df = df.loc[minutes[startIndex:endIndex],:].drop(['Timestamp','Label'],axis=1)
            windowed_df = np.array(windowed_df.values)
        except:
            windowed_df = np.array([])
        return windowed_df

    def __del__(self):
        if os.path.exists(self.__net_sys_stream):
            os.remove(self.__net_sys_stream)