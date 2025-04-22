import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# The `Utilities` class contains attributes used for data preprocessing and feature scaling.
class Utilities:
    def __init__(self):
        # `self.single_value_columns` attribute is a list of column names that represent features with single values. 
        # These columns are excluded from the aggregation dictionary (`self.agg_dict`). 
        self.single_value_columns = ['Fwd PSH Flags', 
                                     'Fwd URG Flags', 
                                     'Bwd URG Flags', 
                                     'URG Flag Cnt', 
                                     'CWE Flag Count', 
                                     'ECE Flag Cnt', 
                                     'Fwd Byts/b Avg', 
                                     'Fwd Pkts/b Avg', 
                                     'Fwd Blk Rate Avg', 
                                     'Bwd Byts/b Avg', 
                                     'Bwd Pkts/b Avg', 
                                     'Bwd Blk Rate Avg', 
                                     'Init Fwd Win Byts', 
                                     'Fwd Seg Size Min' ]
        
        # `agg_dict` contains features for Network-level data with statistical method applied to each feature
        # for aggreagtion of network-level data to convert `Timestamp` from milliseconds resolution to seconds resolution .
        self.agg_dict = {
                            'Flow Duration'     : 'mean',
                            'Tot Fwd Pkts'      : 'sum',
                            'Tot Bwd Pkts'      : 'sum',
                            'TotLen Fwd Pkts'   : 'mean',
                            'TotLen Bwd Pkts'   : 'mean',
                            'Fwd Pkt Len Max'   : 'max',
                            'Fwd Pkt Len Min'   : 'min',
                            'Fwd Pkt Len Mean'  : 'mean',
                            'Fwd Pkt Len Std'   : 'mean',
                            'Bwd Pkt Len Max'   : 'max',
                            'Bwd Pkt Len Min'   : 'min',
                            'Bwd Pkt Len Mean'  : 'mean',
                            'Bwd Pkt Len Std'   : 'mean',
                            'Flow Byts/s'       : 'mean',
                            'Flow Pkts/s'       : 'mean',
                            'Flow IAT Mean'     : 'mean',
                            'Flow IAT Std'      : 'mean',
                            'Flow IAT Max'      : 'max',
                            'Flow IAT Min'      : 'min',
                            'Fwd IAT Tot'       : 'mean',
                            'Fwd IAT Mean'      : 'mean',
                            'Fwd IAT Std'       : 'mean',
                            'Fwd IAT Max'       : 'max',
                            'Fwd IAT Min'       : 'min',
                            'Bwd IAT Tot'       : 'mean',
                            'Bwd IAT Mean'      : 'mean',
                            'Bwd IAT Std'       : 'mean',
                            'Bwd IAT Max'       : 'max',
                            'Bwd IAT Min'       : 'min',
                            'Fwd PSH Flags'     : 'mean',
                            'Bwd PSH Flags'     : 'mean',
                            'Fwd URG Flags'     : 'mean',
                            'Bwd URG Flags'     : 'mean',
                            'Fwd Header Len'    : 'mean',
                            'Bwd Header Len'    : 'mean',
                            'Fwd Pkts/s'        : 'mean',
                            'Bwd Pkts/s'        : 'mean',
                            'Pkt Len Min'       : 'min',
                            'Pkt Len Max'       : 'max',
                            'Pkt Len Mean'      : 'mean',
                            'Pkt Len Std'       : 'mean',
                            'Pkt Len Var'       : 'mean',
                            'FIN Flag Cnt'      : 'sum',
                            'SYN Flag Cnt'      : 'sum',
                            'RST Flag Cnt'      : 'sum',
                            'PSH Flag Cnt'      : 'sum',
                            'ACK Flag Cnt'      : 'sum',
                            'URG Flag Cnt'      : 'sum',
                            'CWE Flag Count'    : 'sum',
                            'ECE Flag Cnt'      : 'sum',
                            'Down/Up Ratio'     : 'mean',
                            'Pkt Size Avg'      : 'mean',
                            'Fwd Seg Size Avg'  : 'mean',
                            'Bwd Seg Size Avg'  : 'mean',
                            'Fwd Byts/b Avg'    : 'mean',
                            'Fwd Pkts/b Avg'    : 'mean',
                            'Fwd Blk Rate Avg'  : 'mean',
                            'Bwd Byts/b Avg'    : 'mean',
                            'Bwd Pkts/b Avg'    : 'mean',
                            'Bwd Blk Rate Avg'  : 'mean',
                            'Subflow Fwd Pkts'  : 'mean',
                            'Subflow Fwd Byts'  : 'mean',
                            'Subflow Bwd Pkts'  : 'mean',
                            'Subflow Bwd Byts'  : 'mean',
                            'Init Fwd Win Byts' : 'mean',
                            'Init Bwd Win Byts' : 'mean',
                            'Fwd Act Data Pkts' : 'sum',
                            'Fwd Seg Size Min'  : 'min',
                            'Active Mean'       : 'mean',
                            'Active Std'        : 'mean',
                            'Active Max'        : 'max',
                            'Active Min'        : 'min',
                            'Idle Mean'         : 'mean',
                            'Idle Std'          : 'mean',
                            'Idle Max'          : 'max',
                            'Idle Min'          : 'min'
                        }
        
        for val in self.single_value_columns: del self.agg_dict[val]
        self.columns_to_scale = list(self.agg_dict.keys())

        # `self.key_columns` attribute is used as key for aggregation of data 
        # to convert `Timestamp` from milliseconds resolution to seconds resolution.
        self.key_columns = ["Timestamp", 
                            "Protocol", 
                            "Src Port", 
                            "Dst Port"]

        # `system_columns` contains features for System-level data.
        self.system_columns = ['Memory(% Committed Bytes In Use)', 
                               'Network Interface(Current Bandwidth)', 
                               'PhysicalDisk(% Disk Time)', 
                               'PhysicalDisk(Disk Reads/sec)', 
                               'PhysicalDisk(Disk Writes/sec)', 
                               'PhysicalDisk(Current Disk Queue Length)', 
                               'Processor(% Processor Time)', 
                               'Processor Information(% Processor Performance)', 
                               'Processor Information(% Processor Utility)', 
                               'Processor Information(% Processor Time)']

# `LoadDataset` class loads network and system data from specified folders in a dataset.
class LoadDataset:
    def __init__(self,folder):
        """ LOAD NETWORK & SYSTEM DATA """
        data = self.__load_Network_System_from(folder)
        self.network = data[0]
        self.system  = data[1]

    def __load_Network_System_from(self, location):
        folder = os.path.join(os.getcwd(),f"Sample Dataset/{location}")

        """ LOAD NETWORK DATA """
        network_csv = []
        network = os.path.join(folder, f"NETWORK")
        for file in os.listdir(network):
            file_path = os.path.join(network, file)
            network_csv.append(pd.read_csv(file_path))

        """ LOAD SYSTEM DATA """
        system_csv  = []
        system  = os.path.join(folder, f"SYSTEM")
        for file in os.listdir(system):
            file_path = os.path.join(system, file)
            system_csv.append(pd.read_csv(file_path))

        return (network_csv, system_csv)
        
# `Normalization` class defines methods for preparing and applying min-max and z-score normalization 
# on network and system-level features of a dataset.
class Normalization(Utilities):
    def __init__(self):
        super().__init__()
        network_part, system_part = self.__prepareNormalizationSetterDataset()
        self.__prepareNetworkScaler(network_part)
        self.__prepareSystemScaler(system_part)
            
    def __prepareNormalizationSetterDataset(self):
        """
        The function uses combined dataset (Raw_Dataset.csv) for creating normalization objects by reading a CSV file, 
        and separating Network part & System Part.
        """
        file_path = os.path.join(os.getcwd(),"Dataset/Raw_Dataset.csv")
        normalizationSetterDataset = pd.read_csv(file_path).drop(self.single_value_columns, axis=1)
        network_part = normalizationSetterDataset.drop(self.key_columns+self.system_columns+["Label"], axis=1)
        system_part  = normalizationSetterDataset[self.system_columns]
        return (network_part,system_part)

    def __prepareNetworkScaler(self, network_part):
        """
        This function creates a MinMaxScaler object and fits it to a Network-level data of entire dataset.
        """
        self.network_scaler = MinMaxScaler()
        self.network_scaler.fit(network_part)
    
    def prepareSystemScaler(self,system_part):
        """
        This function creates a StandardScaler object and fits it to a System-level data of entire dataset.
        """
        self.system_scaler = StandardScaler()
        self.system_scaler.fit(system_part)
        
    def min_max_normalization(self, df):
        """
        This function `min_max_normalization` performs min-max normalization on a DataFrame only on Network-level features.
        """
        _df = df.drop(self.key_columns, axis=1)
        scaled_df = self.network_scaler.transform(_df)
        scaled_df = pd.DataFrame(scaled_df, columns = self.columns_to_scale)
        df = pd.concat([df[self.key_columns],scaled_df], axis=1)
        return df

    def z_score_normalization(self,df):
        """
        This function `z_score_normalization` performs z-score normalization on a DataFrame only on System-level features.
        """
        _df = df.drop(['Timestamp'], axis=1)
        scaled_df = self.system_scaler.transform(_df)
        scaled_df = pd.DataFrame(scaled_df, columns = self.system_columns)
        df = pd.concat([df['Timestamp'], scaled_df], axis=1)
        return df

class Preprocessing(Normalization):
    def __init__(self):
        super().__init__()

    def __aggregate_NETWORK_DATA_based_on_KEY_COLUMNS(self,df):
        df = df.drop(["Flow ID","Src IP","Dst IP"], axis=1)
        df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
        df = df.drop(["Date","Time"], axis=1)
        df = df.drop(self.single_value_columns,axis=1)
        df = df.groupby(self.key_columns).agg(self.agg_dict)
        return df

    def preprocess_NETWORK_DATA(self, df):
        df = self.__aggregate_NETWORK_DATA_based_on_KEY_COLUMNS(df)
        # Interpolate NULL values
        df["Flow Byts/s"] = df["Flow Byts/s"].interpolate(method='linear')
        # Handling 'inf' values by applying HIGH THRESHOLDING & LOG TRANSFORMATION
        for col in ["Flow Byts/s", "Flow Pkts/s"]:
            # Clip at the 99th percentile
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            max_value = (df[col].quantile(0.99))**10
            df[col] = df[col].fillna(max_value)
            # Applying log transformation
            df[col] = np.log1p(df[col])
        df = df.reset_index()
        df = self.min_max_normalization(df)
        return df

    def preprocess_SYSTEM_DATA(self, df):
        df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
        df = df.drop(["Date","Time"], axis=1)
        for col in self.system_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].interpolate(method='linear')
        df = df.reset_index(drop=True)
        df = self.z_score_normalization(df)
        return df
    
    def merge_network_system(self, network, system, label):
        merged_df = pd.merge(network, system, on='Timestamp', how='left')
        merged_df = merged_df.dropna()
        merged_df = merged_df.reset_index(drop=True)
        merged_df["Label"] = label
        return merged_df