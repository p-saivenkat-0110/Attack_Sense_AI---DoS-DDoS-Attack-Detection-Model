from data_fetcher import *

class Data_Preprocessing(Preprocessing):
    def __init__(self,folder):
        super().__init__()
        self.dataset = Load_Data(folder, self.system_columns)

    def preprocess(self, network_df, system_df, label=-1):
        network = self.preprocess_NETWORK_DATA(network_df)
        system  = self.preprocess_SYSTEM_DATA(system_df)
        merged_network_system = self.merge_network_system(network,system,label)
        return merged_network_system

"""
Below is template code (# commented) for Data Preprocessing...
Set folder name, Run and Boom...
`merged_file` contains data, which is ready to fed Pipeline Model.
This file is saved as `stream.csv` which is used for simulating Real-World Scenerio.
"""

# folder = "NET_SYS"
# preprocessor = Data_Preprocessing(folder)

# network_df = preprocessor.dataset.network
# system_df  = preprocessor.dataset.system
# merged_df  = preprocessor.preprocess(network_df, system_df)

# # print(merged_df.info())
# # print(merged_df.shape)
# merged_df.to_csv(f"./{folder}/net_sys_stream.csv", index = False)