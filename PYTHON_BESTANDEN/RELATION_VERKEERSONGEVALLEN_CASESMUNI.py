import pandas as pd
import numpy as np
import pyodbc
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

logging.info('methodes voor het cleanen van data voorbereiden')

#methodes
def removePrefixProvince(prov):
     provincnaam = prov.split(" ")[1]

     if "-" in provincnaam:
          separetName = provincnaam.split("-")
          provincnaam = "".join(separetName)

     return provincnaam

def changeCases(nmbr):
     if nmbr == '<5':
          return 3
     else:
          return int(nmbr)

def toInt(float):
     return int(float)

def changeProvinceNameToDutch(province):
     if province == "Hainaut":
          return "Henegouwen"
     elif province == "Namur":
          return "Namen"
     elif province == "BrabantWallon":
          return "WaalsBrabant"
     elif province == "Liège":
          return "Luik"
     elif province == "Limbourg":
          return "Limburg"
     elif province == "Luxembourg":
          return "Luxemburg"
     else:
          return province

dictionaryProvincenamesFrenchDutch = {"Hainaut":"Henegouwen", "Namur":"Namen", "BrabantWallon":"WaalsBrabant",'Liège':'Luik','Limbourg':'Limburg','Luxembourg':'Luxemburg'}

logging.info('Methodes zijn gemaakt')

logging.info('Datasets verkeersongevallen inlezen en cleanen')

#datasets inlezen
verkeersongevallenDataset = pd.read_excel("https://statbel.fgov.be/sites/default/files/files/opendata/Verkeersongevallen/TF_ACCIDENTS_2020.xlsx")
besmettingenDataset = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.csv')

#clean na records
verkeersongevallenDataset = verkeersongevallenDataset.dropna(axis=0)
besmettingenDataset = besmettingenDataset.dropna(axis=0)

#set record types
verkeersongevallenDataset['DT_DAY'] = verkeersongevallenDataset['DT_DAY'].astype(np.datetime64)
besmettingenDataset['DATE'] = besmettingenDataset['DATE'].astype(np.datetime64)

#Select the needed variables
verkeersongevallenDataset = verkeersongevallenDataset[["CD_MUNTY_REFNIS","DT_DAY","TX_MUNTY_DESCR_NL","TX_MUNTY_DESCR_FR","TX_PROV_DESCR_NL","TX_RGN_DESCR_NL","MS_ACCT"]]
besmettingenDataset = besmettingenDataset.drop(columns=["TX_ADM_DSTR_DESCR_NL","TX_ADM_DSTR_DESCR_FR"])


# Clean values for matching verkeersongevallen with CASES_MUNI
'''
Problem:
- The PROVINCE variable in CASE_MUNI is written in the local language of the province but the Provincename in verkeersongvallen is written in one language
- NIS5 is a float
- CASE is a string
- Province in verkeersongevallen contains a prefix province

Solution:
- Convert PROVINCE of CASES_MUNI only in dutch 
- NIS5 change it to string
- Change cases to int
- Remove the undeeded prefix
'''
besmettingenDataset['PROVINCE'] = besmettingenDataset['PROVINCE'].map(changeProvinceNameToDutch)
besmettingenDataset['CASES'] = besmettingenDataset['CASES'].map(changeCases)
besmettingenDataset['NIS5'] = besmettingenDataset['NIS5'].map(toInt)
besmettingenDataset = besmettingenDataset[besmettingenDataset["DATE"] < "2020-12-31"]

verkeersongevallenDataset['TX_PROV_DESCR_NL'] = verkeersongevallenDataset['TX_PROV_DESCR_NL'].map(removePrefixProvince)
verkeersongevallenDataset["TX_RGN_DESCR_NL"] = verkeersongevallenDataset["TX_RGN_DESCR_NL"].map({'Vlaams Gewest':'Flanders','Waals Gewest':'Wallonia','Brussels Hoofdstedelijk Gewest':'Brussels'})


# Groupby the same records for both Dataset
verkeersongevallenDataset = verkeersongevallenDataset.groupby(by=["CD_MUNTY_REFNIS","DT_DAY","TX_MUNTY_DESCR_NL","TX_MUNTY_DESCR_FR","TX_PROV_DESCR_NL","TX_RGN_DESCR_NL"]).sum().reset_index()
besmettingenDataset = besmettingenDataset.groupby(by=["NIS5","DATE","TX_DESCR_NL","TX_DESCR_FR","PROVINCE","REGION"]).sum().reset_index()


# CLEAN on date
minDate = min( verkeersongevallenDataset['DT_DAY'].min(), besmettingenDataset['DATE'].min())
maxDate = max( verkeersongevallenDataset['DT_DAY'].max(), besmettingenDataset['DATE'].max())
fullDateDF = pd.DataFrame({'DATE':pd.date_range(minDate,maxDate)})


verkeersongevallenDataset.rename(columns={"DT_DAY":"DATE",'CD_MUNTY_REFNIS':'NIS5','TX_MUNTY_DESCR_NL':'TX_DESCR_NL','TX_RGN_DESCR_NL':'REGION','TX_PROV_DESCR_NL':'PROVINCE','TX_MUNTY_DESCR_FR':'TX_DESCR_FR'}, inplace=True)

besmettingenVerkeersongevalDF = pd.merge(besmettingenDataset,verkeersongevallenDataset, on=['NIS5','DATE','TX_DESCR_NL','TX_DESCR_FR','REGION','PROVINCE'], how='left')
besmettingenVerkeersongevalDF = besmettingenVerkeersongevalDF.fillna(0)

logging.info('Dataset verkeersongevallen ingelzen en gecleant')

logging.info('Proberen connactie te maken met de databank')

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')
cursor = conn.cursor()

logging.info('Connactie succesvol')

logging.info('Tabel BesmettingenVerkeersongeval verwijderen, aanmaken en inlezen')

cursor.execute('DROP TABLE IF EXISTS dbo.BesmettingenVerkeersongeval')

cursor.execute('''
		CREATE TABLE BesmettingenVerkeersongeval (
		     Id int identity(1,1) primary key,
               Date Date,
               Niscode int,
               Municipality nvarchar(50), 
			Province nvarchar(50),
               Region nvarchar(50),
               Cases int,
               Accidents int
			)
               ''')
conn.commit()

for i,row in besmettingenVerkeersongevalDF.iterrows():
            sql = "INSERT INTO dbo.BesmettingenVerkeersongeval (Date, Niscode, Municipality, Province, Region, Cases, Accidents) VALUES (?,?,?,?,?,?,?)"
            cursor.execute(sql, row['DATE'], row['NIS5'], row['TX_DESCR_NL'], row['PROVINCE'], row['REGION'], row['CASES'], row['MS_ACCT']).rowcount
            
conn.commit()

logging.info('Tabel is aangemaakt en ingelezen')
