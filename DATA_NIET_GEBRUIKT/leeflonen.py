import pandas as pd
import pyodbc

dataset = pd.read_csv("./datasets_overzicht/Dataset_FinancieleSteunBelgie.csv")
dataset.dropna(axis=0)
dataset = dataset.groupby(by = ['LABEL_EENHEID']).sum().reset_index()

print("Try to connect database")

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')

print("Connection successful")

cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS dbo.Leeflonen')

cursor.execute('''
		CREATE TABLE Leeflonen (
      MunicipalityNameNL nvarchar(50) primary key,
      AmountFinancialSupport int
			)
               ''')


conn.commit()

print("Start Record insert")
for i,row in dataset.iterrows():
            sql = "INSERT INTO dbo.Leeflonen (MunicipalityNameNL, AmountFinancialSupport) VALUES (?,?)"
            cursor.execute(sql, row['LABEL_EENHEID'], row['TOTAAL']).rowcount
            conn.commit()

print("Record inserted")

# cursor.execute('''
# ALTER TABLE [dbo].[Population] WITH CHECK ADD FOREIGN KEY([MunicipalityNameNL])
# REFERENCES [dbo].[Leeflonen] ([MunicipalityNameNL])
# ''')
print("relationships ok")