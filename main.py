from time import sleep
from datetime import datetime
from queue import Queue
from pipeline_architecture import *
from data_preprocessing import Data_Preprocessing
from collect_network_traffic import *
from collect_system_metrics import *

class HyNetSys:
    def __init__(self, choosen_models):
        model_paths = [
            None,
            "./GRU_models/Layer 2/GRU_1min.h5",
            "./GRU_models/Layer 1/GRU_2min.h5",
            "./GRU_models/Layer 2/GRU_3min.h5",
            "./GRU_models/Layer 1/GRU_4min.h5",
            "./GRU_models/Layer 1/GRU_5min.h5"
        ]

        self.__dataCollectorPath = "NET_SYS"
        self.system_data_collector  = self.__initialize_system_metrics_collector()
        self.network_data_collector = self.__initialize_network_traffic_collector()
        self.fetcher                = self.__initialize_window_fetcher()
        self.data_preprocessor      = self.__initialize_data_preprocessor()
        self.queues                 = self.__initialize_queues(choosen_models)
        self.models                 = self.__initialize_models(model_paths, choosen_models)
        self.executers              = self.__initialize_pipeline(choosen_models)
        self.__observe_network_and_system_data()
    
    def __initialize_system_metrics_collector(self):
        system_data_collector_object = Collect_System_Metrics()
        print("----------------------------------------")
        print("  Initialized System Data Collector...")
        print("----------------------------------------\n")
        return system_data_collector_object

    def __initialize_network_traffic_collector(self):
        network_data_collector_object = Collect_Network_Traffic()
        print("----------------------------------------")
        print("  Initialized Network Data Collector...")
        print("----------------------------------------\n")
        return network_data_collector_object

    def __initialize_window_fetcher(self):
        fetcher = Window_Fetcher()
        print("----------------------------------------")
        print("       Initialized Data Fetcher...")
        print("----------------------------------------\n")
        return fetcher
    
    def __initialize_data_preprocessor(self):
        data_preprocessor = Data_Preprocessing()
        print("----------------------------------------")
        print("    Initialized Data Preprocessor...")
        print("----------------------------------------\n")
        return data_preprocessor

    def __initialize_queues(self, choosen_models):
        queues_required = len(choosen_models)
        queues = []
        for _ in range(queues_required):
            queues.append(Queue())
        queues.append(None)
        print("----------------------------------------")
        print("        Initialized Queue(s)...")
        print("----------------------------------------\n")
        return queues

    def __initialize_models(self, model_paths, choosen_models):
        models = []
        for model in choosen_models:
            models.append(Model(model_paths[model]))
        print("----------------------------------------")
        print("       Models Setup Successful...")
        print("----------------------------------------\n")
        return models

    def __initialize_pipeline(self, choosen_models):
        print("----------------------------------------")
        executers = []
        for index in range(len(choosen_models)):
            exe = Parallel_Executer(
                f"GRU-{choosen_models[index]}min", self.models[index],
                self.fetcher,
                self.queues[index], self.queues[index+1],
                choosen_models[index]
            )
            executers.append(exe)
        print("*")
        print("Pipeline Initialization Successful...")
        print("----------------------------------------\n")
        return executers

    def __observe_network_and_system_data(self):
        print("----------------------------------------------")
        print(self.network_data_collector.name, "=> started... running...")
        self.network_data_collector.start()

        print(self.system_data_collector.name, "=> started... running...")
        self.system_data_collector.start()
        print("----------------------------------------------\n\n")
        print("******************************************************\n**  Observing Network Traffic & System Metrics...   **\n******************************************************\n\n")

    def activate_and_run_pipeline(self):
        print("----------------------------------------")
        for executer in self.executers:
            print(executer.name, "=> started... running...")
            executer.start()
        print("----------------------------------------\n\n")
        print("***************************\n**  Pipeline Activated   **\n***************************\n\n")

    def feedDataToPipeline(self):
        net_sys_update_timer = 0
        status_timer = 0

        while True:
            if(status_timer==0):
                self.pipeline_status()
            status_timer = (status_timer+1)%30
            
            if(net_sys_update_timer==0):
                self.update_net_sys_stream()
            net_sys_update_timer = (net_sys_update_timer+1)%5

            current_timestamp = datetime.now()
            self.queues[0].put(current_timestamp)
            sleep(1)

    def update_net_sys_stream(self):  
        updated_stream = self.data_preprocessor.fetch_latest_data(self.__dataCollectorPath)
        if not updated_stream.empty:
            updated_stream.to_csv(f"./{self.__dataCollectorPath}/net_sys_stream.csv", index = False)

    def pipeline_status(self):
        print("\n--- Model Status ---")
        for model in self.executers:
            status = "✅ Alive" if model.is_alive() else "❌ Dead"
            print(f"[{model.name}]: {status}")
        print("--------------------------------------------\n")
        for index, queue in enumerate(self.queues,1):
            if queue:
                print(f"Q{index} -> {queue.qsize()}")
        print("--------------------------------------------\n\n")
try:
    print()
    print("**************************")
    print("*                        *")
    print("*        HYNETSYS        *")
    print("*                        *")
    print("**************************")
    print()

    print("-----------------------------------------------------------")
    print("WELCOME!! CUSTOMIZE ARCHITECTURE BASED ON YOUR REQUIREMENTS")
    print("-----------------------------------------------------------\n")

    print("Choose models by entering numbers:\n *")
    print("(1) ->  1-Min Model")
    print("(2) ->  2-Min Model")
    print("(3) ->  3-Min Model")
    print("(4) ->  4-Min Model")
    print("(5) ->  5-Min Model")
    print(" *\nPress (0) -> To save architecture & start...\n")
    choosen_models = []
    
    while True:
        try:
            model_number = int(input("Enter your choice : "))
        except:
            raise Exception("Invalid input...")

        if(model_number==0):
            if choosen_models:
                print("\n==> Architecture Saved!! Starting...")
                break
            else:
                raise Exception("No models choosen...")

        if len(choosen_models)==5:
            print("\n==> All models choosed!! Press (0) -> To save architecture & start...\n")
            continue

        if(model_number in [1,2,3,4,5]):
            if(model_number in choosen_models):
                print(f"\n==> {model_number}-Min Model already added!!\n")
            else:
                choosen_models.append(model_number)
                print(f"\t\t\t---------- {model_number}-Min Model added ----------")
                if len(choosen_models)==5:
                    print("\n==> All models choosed!! Press (0) -> To save architecture & start...\n")
        else:
            print("\n==> Invalid choice!!\n")

    print()
    print()
    hynetsys_object = HyNetSys(choosen_models)
    hynetsys_object.activate_and_run_pipeline()
    hynetsys_object.feedDataToPipeline()    
    
except KeyboardInterrupt:
    print("Exiting pipeline...")
    hynetsys_object.system_data_collector.stop_collection()
    hynetsys_object.network_data_collector.append_to_network_stream()
except Exception as error:
    print(error)