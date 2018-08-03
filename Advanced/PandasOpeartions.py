import pandas as pd

XYZ_web = {'Day': [1, 2, 3, 4, 5, 6], "Visitors": [1000, 700, 6000, 1000, 400, 350],
           "Bounce_Rate": [20, 20, 23, 15, 10, 34]}

df = pd.DataFrame(XYZ_web)
print(df)
df.set_index("Day", inplace=True)
print("After setting index on day \n", df)
print("First two rows", df.head(2))
print("Last two rows", df.tail(2))

df1 = pd.DataFrame({"HPI": [80, 90, 70, 60], "Int_Rate": [2, 1, 2, 3], "IND_GDP": [50, 45, 45, 67]},
                   index=[2001, 2002, 2003, 2004])

df2 = pd.DataFrame({"HPI": [80, 90, 70, 60], "Int_Rate": [2, 1, 2, 3], "IND_GDP": [50, 45, 45, 67]},
                   index=[2005, 2002, 2007, 2008])

merged = pd.merge(df1, df2)
print("Merged frame", merged)

merged = pd.merge(df1, df2, on="HPI")
print("Merged frame-2", merged)

concat = pd.concat([df1, df2])
print("Concatenation of", concat)

concat = pd.concat([df1, df2], axis=1)
print(concat)

data = pd.read_excel("D:\\1_AWS\\CloudFormation.xlsx", index_col=0)
data.to_html("D:\\1_AWS\\CloudFormation.html")