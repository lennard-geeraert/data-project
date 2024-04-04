import numpy as np
import pandas as pd
import pyodbc
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

logging.info("Datasets mort en deaths volgens leeftijdsgroep inlezen en cleanen")

# -------------------------------- group by en drop not usefull columns -----------------------

mortDataSet = pd.read_csv("https://epistat.sciensano.be/Data/COVID19BE_MORT.csv")
mortDataSet = mortDataSet.dropna(axis=0)

mortDataSet = mortDataSet.drop("SEX", axis="columns")

mortDataSet = mortDataSet.groupby(by = ["DATE", "REGION", "AGEGROUP"]).sum().reset_index()

mortDataSet['DATE'] = mortDataSet['DATE'].astype(np.datetime64)

# --------

deathsDataSet = pd.read_excel("https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.xlsx")
deathsDataSet.dropna(axis=0)

deathsDataSet = deathsDataSet[deathsDataSet['DT_DATE'] >= "2020-03-01"]

deathsDataSet = deathsDataSet.drop(["CD_ARR", "CD_PROV", "NR_YEAR", "NR_WEEK", "CD_SEX"], axis="columns")

mapping_dict_regio = {2000 : 'Flanders', 3000 : 'Wallonia', 4000 : 'Brussels'}
deathsDataSet['CD_REGIO'] = deathsDataSet['CD_REGIO'].map(mapping_dict_regio)

deathsDataSet = deathsDataSet.groupby(by = ["DT_DATE", "CD_REGIO", "CD_AGEGROUP"]).sum().reset_index()

def toString(lijn):
    return str(lijn)

deathsDataSet['DATE'] = deathsDataSet['DT_DATE']
deathsDataSet['REGION'] = deathsDataSet['CD_REGIO']
deathsDataSet['AGEGROUP'] = deathsDataSet['CD_AGEGROUP']
deathsDataSet = deathsDataSet.drop(["DT_DATE", "CD_REGIO", "CD_AGEGROUP"], axis="columns")

# -------------------------------- Clean features --------------------------------

minDate = min(mortDataSet['DATE'].min(), deathsDataSet['DATE'].min())
maxDate = max(mortDataSet['DATE'].max(), deathsDataSet['DATE'].max())

fullDateDF = pd.DataFrame({'DATE':pd.date_range(minDate, maxDate)})
fullDateDF['KEY'] = 0

fullRegionDF = pd.DataFrame({'REGION':mortDataSet['REGION'].unique()})
fullRegionDF['KEY'] = 0

fullAgeGroupDF = pd.DataFrame({'AGEGROUP':mortDataSet['AGEGROUP'].unique()})
fullAgeGroupDF['KEY'] = 0

fullDateRegion = pd.merge(fullDateDF, fullRegionDF, on='KEY', how='outer')
fullDateRegion = fullDateRegion.drop(columns=['KEY'], axis=1)

fullDateRegionAgeGroupDF = pd.merge(fullDateDF, fullRegionDF.merge(fullAgeGroupDF, on='KEY', how="outer"), on='KEY', how="outer")
fullDateRegionAgeGroupDF = fullDateRegionAgeGroupDF.drop(columns=['KEY'],axis=1)

deathsDataSet = fullDateRegionAgeGroupDF.merge(deathsDataSet, on=["DATE","REGION","AGEGROUP"], how='left')
deathsDataSet = deathsDataSet.fillna(0)

mortDataSet = fullDateRegionAgeGroupDF.merge(mortDataSet, on=["DATE","REGION","AGEGROUP"], how='left')
mortDataSet = mortDataSet.fillna(0)

def toString(lijn):
    return str(lijn)

deathsDataSet['PKDeaths_AgeGroup'] = deathsDataSet['DATE'].map(toString) + deathsDataSet['REGION'] + deathsDataSet['AGEGROUP']
mortDataSet['PKMort_AgeGroup'] = mortDataSet['DATE'].map(toString) + mortDataSet['REGION'] + mortDataSet['AGEGROUP']

logging.info("Datasets ingelzen en gecleant")

# -------------------------------- make connection with database ------------------------------------------

logging.info("Connactie maken met de database")

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')

logging.info("Connectie succesvol")

cursor = conn.cursor()

# ------------------------------------- dataset deathsTotal_AgeGroup inlezen ---------------------------------

logging.info("Tabel DeathsTotal_AgeGroup verwijderen, aanmaken en inlezen")

cursor.execute('DROP TABLE IF EXISTS dbo.DeathsTotal_AgeGroup')

cursor.execute('''
		CREATE TABLE DeathsTotal_AgeGroup (
            DateDeaths Date,
            RegionDeaths nvarchar(50),
            AgeGroupDeaths nvarchar(50),
            DeathsDeaths int,
            PKDeaths_AgeGroup nvarchar(100),
            Constraint PK_DeathsTotal_AgeGroup Primary key (DateDeaths, RegionDeaths, AgeGroupDeaths)
			)
               ''')

conn.commit()

for i,row in deathsDataSet.iterrows():
            sql = "INSERT INTO dbo.DeathsTotal_AgeGroup (DateDeaths, RegionDeaths, AgeGroupDeaths, DeathsDeaths, PKDeaths_AgeGroup) VALUES (?,?,?,?,?)"
            cursor.execute(sql, row['DATE'], row['REGION'], row['AGEGROUP'], row['MS_NUM_DEATH'], row['PKDeaths_AgeGroup']).rowcount

conn.commit()

logging.info("Tabel DeathsTotal_AgeGroup verwijderd, aangemaakt en ingelzen")

# -------------------------------- Dataset Mort_AgeGroup inlezen --------------------------------------------

logging.info("Tabel Mort_AgeGroup verwijderen, aanmaken en inlezen")

cursor.execute('DROP TABLE IF EXISTS dbo.Mort_AgeGroup')

cursor.execute('''
		CREATE TABLE Mort_AgeGroup (
            DateMort Date,
            RegionMort nvarchar(50),
            AgeGroupMort nvarchar(50),
            DeathsMort int null,
            PKMort_AgeGroup nvarchar(100),
            Constraint PK_Mort_AgeGroup Primary key (DateMort, RegionMort, AgeGroupMort)
			)
               ''')

conn.commit()

for i,row in mortDataSet.iterrows():
            sql = "INSERT INTO dbo.Mort_AgeGroup (DateMort, RegionMort, AgeGroupMort, DeathsMort, PKMort_AgeGroup) VALUES (?,?,?,?,?)"
            cursor.execute(sql, row['DATE'], row['REGION'], row['AGEGROUP'], row['DEATHS'], row['PKMort_AgeGroup']).rowcount
            
conn.commit()

logging.info("Tabel Mort_AgeGroup verwijderd, aangemaakt en ingelezen")

# -------------------------------------- Make a relation with foreign key ------------------------------------------------

logging.info('Relaties maken tussen de tabellen DeathsTotalAgeGroup en Mort_AgeGroup')

cursor.execute('''
ALTER TABLE [dbo].[DeathsTotal_AgeGroup]
ADD CONSTRAINT FK_DeathsTotal_AgeGroup
FOREIGN KEY(DateDeaths, RegionDeaths, AgeGroupDeaths)
REFERENCES [dbo].[Mort_AgeGroup] (DateMort, RegionMort, AgeGroupMort)
ON DELETE CASCADE
''')

conn.commit()

logging.info("Relaties zijn gelegd")