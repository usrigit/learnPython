import pandas as pd

# Create a DataFrame
d = {
    'Name': [None, 'Bobby', 'jodha', 'jack', 'raghu', 'Cathrine',
             'Alisa', 'Bobby', 'kumar', 'Alisa', 'Alex', 'Cathrine'],
    'Age': ['26,0', 24, 23, 22, None, 24, 26, 24, 22, None, 24, 24],

    'Score': ['85,5', 63, 55, 74, 31, 77, 85, 63, 42, 62, 89, 77]}

df = pd.DataFrame(d, columns=['Name', 'Age', 'Score'])
print(df)
cols = ['Age', 'Score']
df[cols] = df[cols].replace({'\$': '', ',': ''}, regex=True)
# Drop a row by condition
new_df = df[df['Age'].notnull()]

print("===================================\n")
print(new_df)