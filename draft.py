import pandas as pd
import numpy as np

df = pd.read_csv("iterable_backfill_test.csv")
# print(df.values)

nan_values = df.isna()
# print(nan_values)

df.fillna("", inplace=True)
# print(df.values)

all_data = []
for x in df.values:
    user_data = []
    email_dic = {}
    phone_dic = {}
    user_arr = []
    for y in x:
        if y == x[2] and y != "":
            y = "+" + str(y).split(".")[0]
        user_data.append(y)
    email_dic[user_data[0]] = user_data[1]
    phone_dic[user_data[2]] = user_data[3]
    user_arr.append(email_dic)
    user_arr.append(phone_dic)
    all_data.append(user_arr)

print(all_data)

