import pandas as pd
inputFile = "../../data/train.csv"
trainFileHeader = "week_number,sales_depot_id,sales_channel_id,route_id,client_id,product_id,sales_units,sales,return_units,return,demand";
print(trainFileHeader.split(','))
for i,chunk in enumerate(pd.read_csv(inputFile, chunksize=10000, names = trainFileHeader.split(','), skiprows=2)):
    chunk.to_csv('../../data/chunks2/chunk{}.csv'.format(i))