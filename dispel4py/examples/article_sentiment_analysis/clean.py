'''
This is the cleaner program for data in "Articles.txt"
'''

import pandas

df = pandas.read_csv("Articles.csv", encoding="ISO-8859-1")
# print(df.head())
# print(df.dtypes)
# print("AAA")

print(f"lines before drop na:{df.count()}")
df = df.dropna()
print(f"lines after drop na:{df.count()}")


# data clean
df["Article"] = df["Article"].str.replace(r"<\/*br\/*>", "",regex=True)
df["Article"] = df["Article"].str.replace(r"</strong>*", "",regex=True)
df["Article"] = df["Article"].str.replace("strong>", "")
df["Article"] = df["Article"].str.replace(r'<span.*span>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'<a.*a>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'<img.*img>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'<link.*link>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'<script.*script>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'<link.*/>', "", regex=True)
df["Article"] = df["Article"].str.replace(r'img alt.*http.*"/', "", regex=True)
df["Article"] = df["Article"].str.replace("&amp;", "&")
df["Article"] = df["Article"].str.replace("\r\n", "")

# new column location
locationCol = df["Article"].str.split(":",1,True)
df["Location"] = locationCol[0].str.upper()
df.drop(columns=["Article"],inplace=True)
df["Article"] = locationCol[1]
# df.columns = ["Location","Article","Date","Heading","NewsType"]

df.to_csv("Articles_cleaned.csv", index_label=False, header= None, index = False, sep='\t' )

print(df.head(5))