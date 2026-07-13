
def new_data_path():
    parent_dir='/home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter'
    last_trained_data=''

    with open(f'{parent_dir}/Metadata/Last_Trained_data.txt','r') as f:
        last_trained_data=f.read()

    if 'Training_data' in last_trained_data:
        last_batch=int(last_trained_data[14])
    else:
        last_batch=-1

    if last_batch!=3:
        new_batch=last_batch+1
        with open(f'{parent_dir}/Metadata/Last_Trained_data.txt','w') as f:
            f.write(f'Training_data_{new_batch}.parquet')

        return(f'{parent_dir}/Data/Training_data/Training_data_{new_batch}.parquet')
    else:
        return('No new data batch')


# Training_data_0.parquet