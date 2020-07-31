import pandas as pd

df = pd.DataFrame({"A": [1,2,3], "B": [4,5,6], "C": [7,8,9]})

print(df)


for i, row in df.iterrows():
    row["C"] = row["A"] + row["B"]

print(df)
