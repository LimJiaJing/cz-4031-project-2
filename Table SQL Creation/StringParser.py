import pandas as pd

fileName = ['customer.csv','lineitem.csv', 'nation.csv', 'orders.csv', 'part.csv', 'partsupp.csv', 'region.csv', 'supplier.csv']
for i in range(len(fileName)):
    data = pd.read_csv(fileName[i], sep='delimiter', header=None)
    # print(data[0])
    # new data frame with split value columns
    new = data[0].str.split("|", expand = True)
    new.to_csv(fileName[i].replace('.csv', '_new.csv'), index=False,header=False,columns = list(range(len(new.columns) - 1)))
    print(fileName[i] + "_new is done")
