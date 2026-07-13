import shutil
import os

l=['/home/victus/Fahad/Kafka/App_Traffic_Monitor/artifacts',
   '/home/victus/Fahad/Kafka/App_Traffic_Monitor/metadata',
   '/home/victus/Fahad/Kafka/App_Traffic_Monitor/App_Traffic_Delta']

for p in l:
    directory_to_remove = p

    if os.path.exists(directory_to_remove):
        try:
            shutil.rmtree(directory_to_remove)
            print(f"Directory '{directory_to_remove}' and its contents removed successfully.")
        except OSError as e:
            print(f"Error: {directory_to_remove} : {e.strerror}")
    else:
        print(f"Directory '{directory_to_remove}' does not exist.")

