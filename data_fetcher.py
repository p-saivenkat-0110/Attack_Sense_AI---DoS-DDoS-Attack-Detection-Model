import os
import pandas as pd
import numpy as np
import joblib

# The `Utilities` class contains attributes used for data preprocessing and feature scaling.
class Utilities:
    def __init__(self):
        # `self.single_value_columns` attribute is a list of column names that represent features with single values. 
        # These columns are excluded from the aggregation dictionary (`self.agg_dict`). 
        self.single_value_columns = ['Fwd PSH Flags', 
                                     'Fwd URG Flags', 
                                     'Bwd URG Flags', 
                                     'URG Flag Count', 
                                     'CWR Flag Count', 
                                     'ECE Flag Count', 
                                     'Fwd Bytes/Bulk Avg', 
                                     'Fwd Packet/Bulk Avg', 
                                     'Fwd Bulk Rate Avg', 
                                     'Bwd Bytes/Bulk Avg', 
                                     'Bwd Packet/Bulk Avg', 
                                     'Bwd Bulk Rate Avg', 
                                     'FWD Init Win Bytes', 
                                     'Fwd Seg Size Min' ]
        

        # `agg_dict` contains features for Network-level data with statistical method applied to each feature
        # for aggreagtion of network-level data to convert `Timestamp` from milliseconds resolution to seconds resolution .
        self.agg_dict = {
                            'Flow Duration'              : 'mean',
                            'Total Fwd Packet'           : 'sum',
                            'Total Bwd packets'          : 'sum',
                            'Total Length of Fwd Packet' : 'mean',
                            'Total Length of Bwd Packet' : 'mean',
                            'Fwd Packet Length Max'      : 'max',
                            'Fwd Packet Length Min'      : 'min',
                            'Fwd Packet Length Mean'     : 'mean',
                            'Fwd Packet Length Std'      : 'mean',
                            'Bwd Packet Length Max'      : 'max',
                            'Bwd Packet Length Min'      : 'min',
                            'Bwd Packet Length Mean'     : 'mean',
                            'Bwd Packet Length Std'      : 'mean',
                            'Flow Bytes/s'               : 'mean',
                            'Flow Packets/s'             : 'mean',
                            'Flow IAT Mean'              : 'mean',
                            'Flow IAT Std'               : 'mean',
                            'Flow IAT Max'               : 'max',
                            'Flow IAT Min'               : 'min',
                            'Fwd IAT Total'              : 'mean',
                            'Fwd IAT Mean'               : 'mean',
                            'Fwd IAT Std'                : 'mean',
                            'Fwd IAT Max'                : 'max',
                            'Fwd IAT Min'                : 'min',
                            'Bwd IAT Total'              : 'mean',
                            'Bwd IAT Mean'               : 'mean',
                            'Bwd IAT Std'                : 'mean',
                            'Bwd IAT Max'                : 'max',
                            'Bwd IAT Min'                : 'min',
                            'Fwd PSH Flags'              : 'mean',
                            'Bwd PSH Flags'              : 'mean',
                            'Fwd URG Flags'              : 'mean',
                            'Bwd URG Flags'              : 'mean',
                            'Fwd Header Length'          : 'mean',
                            'Bwd Header Length'          : 'mean',
                            'Fwd Packets/s'              : 'mean',
                            'Bwd Packets/s'              : 'mean',
                            'Packet Length Min'          : 'min',
                            'Packet Length Max'          : 'max',
                            'Packet Length Mean'         : 'mean',
                            'Packet Length Std'          : 'mean',
                            'Packet Length Variance'     : 'mean',
                            'FIN Flag Count'             : 'sum',
                            'SYN Flag Count'             : 'sum',
                            'RST Flag Count'             : 'sum',
                            'PSH Flag Count'             : 'sum',
                            'ACK Flag Count'             : 'sum',
                            'URG Flag Count'             : 'sum',
                            'CWR Flag Count'             : 'sum',
                            'ECE Flag Count'             : 'sum',
                            'Down/Up Ratio'              : 'mean',
                            'Average Packet Size'        : 'mean',
                            'Fwd Segment Size Avg'       : 'mean',
                            'Bwd Segment Size Avg'       : 'mean',
                            'Fwd Bytes/Bulk Avg'         : 'mean',
                            'Fwd Packet/Bulk Avg'        : 'mean',
                            'Fwd Bulk Rate Avg'          : 'mean',
                            'Bwd Bytes/Bulk Avg'         : 'mean',
                            'Bwd Packet/Bulk Avg'        : 'mean',
                            'Bwd Bulk Rate Avg'          : 'mean',
                            'Subflow Fwd Packets'        : 'mean',
                            'Subflow Fwd Bytes'          : 'mean',
                            'Subflow Bwd Packets'        : 'mean',
                            'Subflow Bwd Bytes'          : 'mean',
                            'FWD Init Win Bytes'         : 'mean',
                            'Bwd Init Win Bytes'         : 'mean',
                            'Fwd Act Data Pkts'          : 'sum',
                            'Fwd Seg Size Min'           : 'min',
                            'Active Mean'                : 'mean',
                            'Active Std'                 : 'mean',
                            'Active Max'                 : 'max',
                            'Active Min'                 : 'min',
                            'Idle Mean'                  : 'mean',
                            'Idle Std'                   : 'mean',
                            'Idle Max'                   : 'max',
                            'Idle Min'                   : 'min'
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
class Load_Data:
    def __init__(self,folder,system_columns):
        """ LOAD NETWORK & SYSTEM DATA """
        data = self.__load_Network_System_from(folder,system_columns)
        self.network = data[0]
        self.system  = data[1]

    def __load_Network_System_from(self, location, system_columns):
        folder = os.path.join(os.getcwd(),f"{location}")

        """ LOAD NETWORK DATA """
        network_df  = None
        network_csv = os.path.join(folder, r"NETWORK\network_stream.csv")
        if os.path.isfile(network_csv):
            network_df = pd.read_csv(network_csv)
            network_df['Timestamp'] = pd.to_datetime(network_df['Timestamp'], format="%m/%d/%Y %I:%M:%S %p")
            network_df['Timestamp'] = network_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")

        """ LOAD SYSTEM DATA """
        system_df  = None
        system_csv = os.path.join(folder, r"SYSTEM\system_stream.csv")
        if os.path.isfile(system_csv):
            system_df = pd.read_csv(system_csv)
            system_df.columns = ['Timestamp']+system_columns
            system_df['Timestamp'] = pd.to_datetime(system_df['Timestamp'], format="%d/%m/%Y %H:%M:%S.%f", errors='coerce')
            system_df['Timestamp'] = system_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")
        return (network_df, system_df)
        
# `Normalization` class defines methods for preparing and applying min-max and z-score normalization 
# on network and system-level features of a dataset.
class Normalization(Utilities):
    def __init__(self):
        super().__init__()
        self.__prepareNetworkScaler()
        self.__prepareSystemScaler()

    def __prepareNetworkScaler(self):
        """
        This function creates a MinMaxScaler object and fits it to a Network-level data of entire dataset.
        """
        self.network_scaler = joblib.load(r".\Scalers\MinMaxScaler.save")
    
    def __prepareSystemScaler(self):
        """
        This function creates a StandardScaler object and fits it to a System-level data of entire dataset.
        """
        self.system_scaler = joblib.load(r".\Scalers\StandardScaler.save")
        
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

    def __aggregate_NETWORK_DATA_based_on_KEY_COLUMNS(self, df):
        df = df.drop(["Flow ID","Src IP","Dst IP"], axis=1)
        # df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
        # df = df.drop(["Date","Time"], axis=1)
        df = df.drop(self.single_value_columns,axis=1)
        df = df.groupby(self.key_columns).agg(self.agg_dict)
        return df

    def preprocess_NETWORK_DATA(self, df):
        df = self.__aggregate_NETWORK_DATA_based_on_KEY_COLUMNS(df)
        # Interpolate NULL values
        df["Flow Bytes/s"] = df["Flow Bytes/s"].interpolate(method='linear')
        # Handling 'inf' values by applying HIGH THRESHOLDING & LOG TRANSFORMATION
        for col in ["Flow Bytes/s", "Flow Packets/s"]:
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
        # df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
        # df = df.drop(["Date","Time"], axis=1)
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