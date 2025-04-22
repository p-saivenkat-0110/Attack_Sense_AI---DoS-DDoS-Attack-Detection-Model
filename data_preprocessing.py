from data_fetcher import *

class DataPreprocessing(Preprocessing):
    def __init__(self,folder):
        super().__init__()
        self.dataset = LoadDataset(folder)

    def preprocess_NETWORK_SYSTEM(self, network_file, system_file):
        network = []
        system  = []
        for network_data, system_data in zip(network_file, system_file):
            network.append(self.preprocess_NETWORK_DATA(network_data))
            system.append(self.preprocess_SYSTEM_DATA(system_data))
        return (network, system)

    def merge_NETWORK_SYSTEM(self, network, system, label):
        merged_data = []
        for network_data, system_data in zip(network, system):
            merged_data.append(self.merge_network_system(network_data,system_data,label))
        return merged_data

    def preprocess(self, network_file, system_file, label=-1):
        network, system = self.preprocess_NETWORK_SYSTEM(network_file, system_file)
        merged_network_system = self.merge_NETWORK_SYSTEM(network, system, label)
        return merged_network_system


"""
Below is template code (# commented) for Data Preprocessing...
Set folder name, Run and Boom...
`merged_file` contains data, which is ready to fed Pipeline Model.
This file is saved as `stream.csv` which is used for simulating Real-World Scenerio.
"""

# folder = "TEST"
# preprocessor = DataPreprocessing(folder)

# network_file = preprocessor.dataset.network
# system_file  = preprocessor.dataset.system
# merged_file  = preprocessor.preprocess(network_file, system_file)

# print(merged_file[0].info())
# print(merged_file[0].shape)
# merged_file[0].to_csv("./Sample Dataset/stream.csv", index = False)