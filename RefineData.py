import pandas as pd


from sklearn import svm
chunksize=500
for chunk in pd.read_csv('train.csv', chunksize):
    print(chunk.head(5))
