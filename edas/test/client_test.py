from distributed import Client
import time

if __name__ == '__main__':
    print( "Starting client")
    client = Client()
    scheduler_info = client.scheduler_info()
    workers = scheduler_info.pop("workers")
    print(" @@@@@@@ SCHEDULER INFO: " + str(scheduler_info ))
    print(f" N Workers: {len(workers)} " )
    time.sleep(30)