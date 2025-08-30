from time import sleep
from datetime import datetime
from queue import Queue
from data_loader import LoadData
from collect_network_traffic import *
from collect_system_metrics import *
from pipeline_architecture import *

class HyNetSys:
    def __init__(self, choosen_models):
        self.shutdown_event = threading.Event()
        model_paths = [
            None,
            "./GRU_models/Layer 2/GRU_1min.h5",
            "./GRU_models/Layer 1/GRU_2min.h5",
            "./GRU_models/Layer 2/GRU_3min.h5",
            "./GRU_models/Layer 1/GRU_4min.h5",
            "./GRU_models/Layer 1/GRU_5min.h5"
        ]

        self.system_data_collector = Collect_System_Metrics()
        print("----------------------------------------")
        print("|  Initialized System Data Collector   |")
        print("----------------------------------------")
                
        self.network_data_collector = Collect_Network_Traffic(self.shutdown_event)
        print("|  Initialized Network Data Collector  |")
        print("----------------------------------------")

        self.load_data = LoadData()
        print("|  Initialized Data Loader             |")
        print("----------------------------------------")

        self.queues = [Queue() for _ in range(len(choosen_models))] + [None]
        print("|  Initialized Queue(s)                |")
        print("----------------------------------------")

        self.models = [Model(model_paths[model]) for model in choosen_models]
        print("|  Models Setup Successful             |")
        print("----------------------------------------\n")

        self.executers = [Parallel_Executer(f"GRU-{choosen_models[index]}min", self.models[index], self.load_data, self.queues[index], self.queues[index+1], choosen_models[index], self.shutdown_event) for index in range(len(choosen_models))]
        print("\n----------------------------------------")
        print("|  Pipeline Initialization Successful  |")
        print("----------------------------------------\n\n")

        self.__observe_network_and_system_data()

    def __observe_network_and_system_data(self):
        print("------------------------------------")
        print(self.network_data_collector.name, "=> running...")
        self.network_data_collector.start()

        print(self.system_data_collector.name,  "=> running...")
        self.system_data_collector.start()
        print("------------------------------------\n")
        print("******************************************************\n**  Observing Network Traffic & System Metrics...   **\n******************************************************\n\n")

    def activate_pipeline(self):
        print("------------------------------")
        for executer in self.executers:
            print(executer.name, "=> running...")
            executer.start()
        print("------------------------------\n")
        print("***************************\n**  Pipeline Activated   **\n***************************\n\n")

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


    def feedDataToPipeline(self):
        net_sys_update_timer = 0
        status_timer = 0

        while not self.shutdown_event.is_set():
            if(status_timer==0):
                self.pipeline_status()
            status_timer = (status_timer+1)%30
            
            if(net_sys_update_timer==0):
                self.load_data.update_net_sys_stream()
            net_sys_update_timer = (net_sys_update_timer+1)%5

            current_timestamp = datetime.now()
            self.queues[0].put(current_timestamp)
            sleep(1)
        
    def run(self):
        self.activate_pipeline()
        self.feedDataToPipeline()

    def __del__(self):
        try:
            self.system_data_collector.stop_collection()
            self.network_data_collector.stop_collection()
            self.network_data_collector.join()
            for executer in self.executers:
                executer.join()
        except:
            pass

def banner():
    print()
    print("**************************")
    print("*                        *")
    print("*        HYNETSYS        *")
    print("*                        *")
    print("**************************")
    print()

def welcome_msg():
    print("-----------------------------------------------------------")
    print("WELCOME!! CUSTOMIZE ARCHITECTURE BASED ON YOUR REQUIREMENTS")
    print("-----------------------------------------------------------\n")

def pipeline_architecture_options():
    print("Choose models by entering numbers:\n *")
    print("(1) ->  1-Min Model")
    print("(2) ->  2-Min Model")
    print("(3) ->  3-Min Model")
    print("(4) ->  4-Min Model")
    print("(5) ->  5-Min Model")
    print(" *\nPress (0) -> To save architecture & start...\n")


def customized_pipeline_architecture():
    choosen_models = []
    while True:
        try:
            model_number = int(input("Enter your choice : "))
        except:
            raise Exception("Invalid input...")

        if(model_number==0):
            if choosen_models:
                print("\n==> Architecture Saved!! Starting...\n")
                break
            else:
                raise Exception("No models choosen...")

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

    return choosen_models

def main():
    try:
        banner()
        welcome_msg()
        pipeline_architecture_options()
        user_defined_architecture = customized_pipeline_architecture()
        hynetsys = HyNetSys(user_defined_architecture)
        hynetsys.run()
        del hynetsys

    except KeyboardInterrupt:
        hynetsys.shutdown_event.set()
        del hynetsys
        print("!! Process Terminated Successfully !!")
    except Exception as e:
        print(e)

if __name__=='__main__':
    main()