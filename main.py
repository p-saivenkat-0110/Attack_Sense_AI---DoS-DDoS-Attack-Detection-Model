from time import sleep
from queue import Queue
from pipeline_architecture import *

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
        
        self.fetcher   = self.__initialize_window_fetcher()
        self.queues    = self.__initialize_queues(choosen_models)
        self.models    = self.__initialize_models(model_paths, choosen_models)
        self.executers = self.__initialize_pipeline(choosen_models)

    def __initialize_window_fetcher(self):
        fetcher = WindowFetcher()
        print("----------------------------------------")
        print("       Initialized Data Fetcher...")
        print("----------------------------------------\n")
        return fetcher

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
            exe = ParallelExecuter(
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

    def activate_pipeline(self):
        print("----------------------------------------")
        for executer in self.executers:
            print(executer.name, "=> started... running...")
            executer.start()
        print("----------------------------------------\n\n")
        print("***************************\n**  Pipeline Activated   **\n***************************\n\n")
    
    def run_pipeline(self):
        while True:
            print("\n--- Model Status ---")
            for model in self.executers:
                status = "✅ Alive" if model.is_alive() else "❌ Dead"
                print(f"[{model.name}]: {status}")
            print("--------------------------------------------\n")
            for index, queue in enumerate(self.queues,1):
                if queue:
                    print(f"Q{index} -> {queue.qsize()}")
            print("--------------------------------------------\n\n")
            sleep(30)

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

    """
    Simulating Real-World fetching of timestamps.
    Timestamps from `stream.csv` file is added to main Queue.
    """
    timestamps = list(set(hynetsys_object.fetcher.tmp['Timestamp']))
    for ts in sorted(timestamps):
        hynetsys_object.queues[0].put(ts)

    hynetsys_object.activate_pipeline()
    hynetsys_object.run_pipeline()
    
    print("hello...")
except KeyboardInterrupt:
    print("Exiting pipeline...")
except Exception as error:
    print(error)
