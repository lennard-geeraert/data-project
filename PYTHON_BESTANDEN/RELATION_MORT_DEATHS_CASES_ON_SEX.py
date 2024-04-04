import pandas as pd
import numpy as np
import pyodbc
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

logging.info("Lezen en cleanen van Mort, Deaths en Cases gegroepeerd volgens gender")

# -------------------------------- group by en drop not usefull columns -----------------------

mortDataSet = pd.read_csv("https://epistat.sciensano.be/Data/COVID19BE_MORT.csv")
mortDataSet = mortDataSet.dropna(axis=0)

mortDataSet = mortDataSet.drop("AGEGROUP", axis="columns")

mortDataSet = mortDataSet.groupby(by = ["DATE", "REGION", "SEX"]).sum().reset_index()

mortDataSet['DATE'] = mortDataSet['DATE'].astype(np.datetime64)

# --------

deathsDataSet = pd.read_excel("https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.xlsx")
deathsDataSet.dropna(axis=0)

deathsDataSet = deathsDataSet[deathsDataSet['DT_DATE'] >= "2020-03-01"]

deathsDataSet = deathsDataSet.drop(["CD_ARR", "CD_PROV", "NR_YEAR", "NR_WEEK", "CD_AGEGROUP"], axis="columns")

mapping_dict_sex = {1 : 'M', 2 : 'F'}
deathsDataSet['CD_SEX'] = deathsDataSet['CD_SEX'].map(mapping_dict_sex)

mapping_dict_regio = {2000 : 'Flanders', 3000 : 'Wallonia', 4000 : 'Brussels'}
deathsDataSet['CD_REGIO'] = deathsDataSet['CD_REGIO'].map(mapping_dict_regio)

deathsDataSet = deathsDataSet.groupby(by = ["DT_DATE", "CD_REGIO", "CD_SEX"]).sum().reset_index()

deathsDataSet['DATE'] = deathsDataSet['DT_DATE']
deathsDataSet['REGION'] = deathsDataSet['CD_REGIO']
deathsDataSet['SEX'] = deathsDataSet['CD_SEX']
deathsDataSet = deathsDataSet.drop(["DT_DATE", "CD_REGIO", "CD_SEX"], axis="columns")

# -----------

casesSex = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv')
casesSex = casesSex.dropna(axis=0)

casesSex = casesSex.drop(["AGEGROUP", "PROVINCE"], axis="columns")

casesSex = casesSex.groupby(by=["DATE", "REGION", "SEX"]).sum().reset_index()

casesSex['DATE'] = casesSex['DATE'].astype(np.datetime64)

# -------------------------------- Clean features --------------------------------

minDate = min(mortDataSet['DATE'].min(), min(casesSex['DATE'].min(), deathsDataSet['DATE'].min()))
maxDate = max(mortDataSet['DATE'].max(), max(casesSex['DATE'].max(), deathsDataSet['DATE'].max()))

fullDateDF = pd.DataFrame({'DATE':pd.date_range(minDate, maxDate)})
fullDateDF['KEY'] = 0

fullRegionDF = pd.DataFrame({'REGION':casesSex['REGION'].unique()})
fullRegionDF['KEY'] = 0

fullSexDF = pd.DataFrame({'SEX':casesSex['SEX'].unique()})
fullSexDF['KEY'] = 0

fullDateRegion = pd.merge(fullDateDF, fullRegionDF, on='KEY', how='outer')
fullDateRegion = fullDateRegion.drop(columns=['KEY'], axis=1)

fullDateRegionSexDF = pd.merge(fullDateDF, fullRegionDF.merge(fullSexDF, on='KEY', how="outer"), on='KEY', how="outer")
fullDateRegionSexDF = fullDateRegionSexDF.drop(columns=['KEY'],axis=1)

casesSex = fullDateRegionSexDF.merge(casesSex, on=["DATE","REGION","SEX"], how='left')
casesSex = casesSex.fillna(0)

mortDataSet = fullDateRegionSexDF.merge(mortDataSet, on=["DATE","REGION","SEX"], how='left')
mortDataSet = mortDataSet.fillna(0)

deathsDataSet = fullDateRegionSexDF.merge(deathsDataSet, on=["DATE","REGION","SEX"], how='left')
deathsDataSet = deathsDataSet.fillna(0)

def toString(lijn):
    return str(lijn)

casesSex['PKCases_Sex'] = casesSex['DATE'].map(toString) + casesSex['REGION'] + casesSex['SEX']
mortDataSet['PKMort_Sex'] = mortDataSet['DATE'].map(toString) + mortDataSet['REGION'] + mortDataSet['SEX']
deathsDataSet['PKDeaths_Sex'] = deathsDataSet['DATE'].map(toString) + deathsDataSet['REGION'] + deathsDataSet['SEX']

logging.info("Mort, Deaths en Cases gegroepeerd volgens gender gecleant en ingeladen")

# -------------------------------- make connection with database ------------------------------------------

logging.info("Proceren connectie te maken met database")

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')

cursor = conn.cursor()

logging.info("Connectie succesvol")

# ---------------------------------- dataset casesagesex inlezen -----------------------------------------------

logging.info("Drop, maak en vullen van tabel Cases_Sex")

cursor.execute('DROP TABLE IF EXISTS dbo.Cases_Sex')

cursor.execute('''
		CREATE TABLE Cases_Sex (
			DateCases Date,
            RegionCases nvarchar(50),
            SexCases nvarchar(50),
            CasesCases int,
            PKCases_Sex nvarchar(100),
            Constraint PK_Cases_Sex Primary key (DateCases, RegionCases, SexCases)
			)
    ''')

conn.commit()

for row in casesSex.itertuples():
    cursor.execute('''
                INSERT INTO Cases_Sex (DateCases, RegionCases, SexCases, CasesCases, PKCases_Sex)VALUES (?,?,?,?,?)''',
                   row.DATE, row.REGION, row.SEX, row.CASES, row.PKCases_Sex)

conn.commit()

logging.info("Cases_Sex ingelezen en aangemaakt")

# ------------------------------------- dataset deathsTotal_AgeGroup inlezen ---------------------------------

logging.info("Table DeathsTotal_Sex verwijderen, aanmaken en inlezen")

cursor.execute('DROP TABLE IF EXISTS dbo.DeathsTotal_Sex')

cursor.execute('''
		CREATE TABLE DeathsTotal_Sex (
            DateDeaths Date,
            RegionDeaths nvarchar(50),
            SexDeaths nvarchar(50),
            DeathsDeaths int,
            PKDeaths_Sex nvarchar(1000),
            Constraint PK_DeathsTotal_Sex Primary key (DateDeaths, RegionDeaths, SexDeaths)
			)
               ''')

conn.commit()

for i,row in deathsDataSet.iterrows():
            sql = "INSERT INTO dbo.DeathsTotal_Sex (DateDeaths, RegionDeaths, SexDeaths, DeathsDeaths, PKDeaths_Sex) VALUES (?,?,?,?,?)"
            cursor.execute(sql, row['DATE'], row['REGION'], row['SEX'], row['MS_NUM_DEATH'], row['PKDeaths_Sex']).rowcount

conn.commit()

logging.info("Tabel DeathsTotal_Sex verwijderd, aangemaakt en ingelezen")

# -------------------------------- Dataset Mort_AgeGroup inlezen --------------------------------------------

logging.info("Tabel Mort_Sex verwijderen, aanmaken en inlezen")

cursor.execute('DROP TABLE IF EXISTS dbo.Mort_Sex')

cursor.execute('''
		CREATE TABLE Mort_Sex (
            DateMort Date,
            RegionMort nvarchar(50),
            SexMort nvarchar(50),
            DeathsMort int null,
            PKMort_Sex nvarchar(100),
            Constraint PK_Mort_Sex Primary key (DateMort, RegionMort, SexMort)
			)
               ''')

conn.commit()

for i,row in mortDataSet.iterrows():
            sql = "INSERT INTO dbo.Mort_Sex (DateMort, RegionMort, SexMort, DeathsMort, PKMort_Sex) VALUES (?,?,?,?,?)"
            cursor.execute(sql, row['DATE'], row['REGION'], row['SEX'], row['DEATHS'], row['PKMort_Sex']).rowcount

conn.commit()

logging.info("Tabel Mort_Sex aangemaakt en ingelezen")

# -------------------------------------- Make a relation with foreign key ------------------------------------------------

logging.info("Relaties leggen tussen de tabellen Mort_Sex, DeathTotal_Sex en Cases_Sex")

cursor.execute('''
ALTER TABLE [dbo].[DeathsTotal_Sex]
ADD CONSTRAINT FK_DeathsTotal_Sex
FOREIGN KEY(DateDeaths, RegionDeaths, SexDeaths)
REFERENCES [dbo].[Mort_Sex] (DateMort, RegionMort, SexMort)
ON DELETE CASCADE
''')

cursor.execute('''
ALTER TABLE [dbo].[Cases_Sex]
ADD CONSTRAINT FK_Cases_Sex
FOREIGN KEY(DateCases, RegionCases, SexCases)
REFERENCES [dbo].[Mort_Sex] (DateMort, RegionMort, SexMort)
ON DELETE CASCADE
''')

conn.commit()

logging.info("Relaties zijn gelegd")