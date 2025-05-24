import os
import pandas as pd

# `Load_Data` class loads network and system data from specified folders in a dataset.
class Load_Data:
    def load_data(self,folder,system_columns):
        """ LOAD NETWORK & SYSTEM DATA """
        return self.__load_Network_System_from(folder,system_columns)

    def __load_Network_System_from(self, folder, system_columns):
        folder = os.path.join(os.getcwd(),f"{folder}")

        """ LOAD NETWORK DATA """
        network_df  = None
        network_csv = os.path.join(folder, r"NETWORK\network_stream.csv")
        if os.path.isfile(network_csv):
            network_df = pd.read_csv(network_csv)
            network_df['Timestamp'] = pd.to_datetime(network_df['Timestamp'], format="%d/%m/%Y %I:%M:%S %p")
            network_df['Timestamp'] = network_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")

        """ LOAD SYSTEM DATA """
        system_df  = None
        system_csv = os.path.join(folder, r"SYSTEM\system_stream.csv")
        if os.path.isfile(system_csv):
            system_df = pd.read_csv(system_csv)
            system_df.columns = ['Timestamp']+system_columns
            system_df['Timestamp'] = pd.to_datetime(system_df['Timestamp'], format="%m/%d/%Y %H:%M:%S.%f", errors='coerce')
            system_df['Timestamp'] = system_df['Timestamp'].dt.strftime("%m/%d/%Y %H:%M:%S")
        return (network_df, system_df)