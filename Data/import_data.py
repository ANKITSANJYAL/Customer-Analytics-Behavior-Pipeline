from ucimlrepo import fetch_ucirepo 
import pandas as pd
import os
  
# fetch dataset 
online_retail = fetch_ucirepo(id=352) 
  
# data (as pandas dataframes) 
X = online_retail.data.features 
y = online_retail.data.targets 

df = pd.concat([X,y],axis=1)

df.to_csv('data.csv',index=False)  
# metadata 
print(online_retail.metadata) 
  
# variable information 
print(online_retail.variables) 
