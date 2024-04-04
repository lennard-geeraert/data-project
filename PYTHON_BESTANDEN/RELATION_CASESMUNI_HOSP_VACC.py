import pandas as pd
import numpy as np
import pyodbc
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

# -------------------------------- methodes ---------------------------------
def filterCases(stringNumber):
    if stringNumber == "<5":
        # kleiner dan vijf wordt beschouwdt als het gemiddelde, daarom kiezen we voor 3
        # we kiezen niet voor 5 omdat 5 alleen al bestaat in de dataset
        return 3
    else:
        return int(stringNumber)

def toString(param):
   return str(param)

logging.info('Start met het inlezen en cleanen van data hosp en vacc')

# -------------------------------- clean HOSP --------------------------------

df_HOSP = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv')
df_HOSP = df_HOSP.dropna(axis=0)
df_HOSP['DATE'] = df_HOSP['DATE'].astype(np.datetime64)

# -------------------------------- clean CASES_MUNI --------------------------------

df_CASES_MUNI = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.csv')
df_CASES_MUNI = df_CASES_MUNI.dropna(axis=0)

df_CASES_MUNI = df_CASES_MUNI.drop(columns=['NIS5',"TX_DESCR_NL","TX_DESCR_FR","TX_ADM_DSTR_DESCR_NL","TX_ADM_DSTR_DESCR_FR"],axis=1)

df_CASES_MUNI['CASES'] = df_CASES_MUNI['CASES'].map(filterCases)

df_CASES_MUNI['DATE'] = df_CASES_MUNI['DATE'].astype(np.datetime64)

# -------------------------------- clean VACC --------------------------------

df_VACC = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_VACC.csv')
df_VACC = df_VACC.dropna(axis=0)
df_VACC = df_VACC.drop(columns=['AGEGROUP','SEX','BRAND','DOSE'],axis=1)

df_VACC['DATE'] = df_VACC['DATE'].astype(np.datetime64)

# -------------------------------- Group by--------------------------------

df_CASES_MUNI = df_CASES_MUNI.groupby(by=['DATE','PROVINCE','REGION']).sum().reset_index()
df_VACC = df_VACC.groupby(by=['DATE','REGION']).sum().reset_index()

# -------------------------------- Clean features --------------------------------

minDate = min( df_HOSP['DATE'].min() , max( df_CASES_MUNI['DATE'].min() , df_VACC['DATE'].min() ) )
maxDate = max( df_HOSP['DATE'].max() , max( df_CASES_MUNI['DATE'].max() , df_VACC['DATE'].max() ) )

fullDateDF = pd.DataFrame({'DATE':pd.date_range(minDate,maxDate)})
fullDateDF['KEY'] = 0

fullProvinceDF = pd.DataFrame({'PROVINCE':df_HOSP['PROVINCE'].unique()})
fullProvinceDF['KEY'] = 0

fullRegionDF = pd.DataFrame({'REGION':['Flanders','Brussels','Wallonia']})
fullRegionDF['KEY'] = 0

fullDateRegion = pd.merge(fullDateDF,fullRegionDF, on='KEY', how='outer')
fullDateRegion = fullDateRegion.drop(columns=['KEY'],axis=1)

fullDateRegionProvinceDF = pd.merge(fullDateDF,fullRegionDF.merge(fullProvinceDF,on='KEY',how="outer"),on='KEY',how="outer")
fullDateRegionProvinceDF = fullDateRegionProvinceDF.drop(columns=['KEY'],axis=1)

df_HOSP = fullDateRegionProvinceDF.merge(df_HOSP,on=["DATE","PROVINCE","REGION"],how='left')
df_HOSP = df_HOSP.fillna(0)

df_CASES_MUNI = fullDateRegionProvinceDF.merge(df_CASES_MUNI,on=["DATE","PROVINCE","REGION"],how='left')
df_CASES_MUNI = df_CASES_MUNI.fillna(0)


df_VACC = fullDateRegion.merge(df_VACC,  on=["DATE","REGION"], how='left')
df_VACC = df_VACC.fillna(0)

logging.info('Data hosp en vacc zijn gecleant')


# --------------------------------  make PK --------------------------------

logging.info("PK aanmaken en zo relaties leggen")

df_CASES_MUNI['PK_CASES_MUNI'] = df_CASES_MUNI['DATE'].map(toString) + df_CASES_MUNI['PROVINCE'] + df_CASES_MUNI['REGION']
df_CASES_MUNI['PK_DATE_REGION'] = df_CASES_MUNI['DATE'].map(toString) + df_CASES_MUNI['PROVINCE'] + df_CASES_MUNI['REGION']

df_HOSP['PK_HOSP'] = df_HOSP['DATE'].map(toString) + df_HOSP['PROVINCE'] + df_HOSP['REGION']

df_CASES_MUNI['PK_CASES_DATE_REGION'] = df_CASES_MUNI['DATE'].map(toString) +  df_CASES_MUNI['REGION']

df_VACC['PK_VACC_DATE_REGION'] = df_VACC['DATE'].map(toString) + df_VACC['REGION']

logging.info("Relaties zijn gelegd")

# -------------------------------- Insert into DB --------------------------------

logging.info('Maak connactie met de databank')

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')
cursor = conn.cursor()

logging.info('Connectie gemaakt met databank')

logging.info('Drop tabellen hosp, casesmunicipality en vaccinationdate als ze bestaan')

cursor.execute('DROP TABLE IF EXISTS dbo.Hosp')
cursor.execute('DROP TABLE IF EXISTS dbo.CasesMunicipality')
cursor.execute('DROP TABLE IF EXISTS dbo.VaccinationDate')

logging.info("Tabellen hosp, casesmunicipality en vaccinationdate zijn gedropt")

logging.info('Maken van de tabellen hosp, casesmunicipality en vaccinationdate')

cursor.execute('''
		CREATE TABLE CasesMunicipality (
			Id nvarchar(75) primary key,
            Date Date,
			Province nvarchar(50),
            Region nvarchar(50),
            Cases int,
            KeyRegionDate nvarchar(75)
			)
               ''')

cursor.execute('''
		CREATE TABLE Hosp (
			Id nvarchar(75) primary key,
            Datum Date,
			Province nvarchar(50),
			Region nvarchar(50),
			NrReporting int,
			TotalIn int,
			TotalInIcu int,
			TotalInResp int,
			TotalInEcmo int,
			NewIn int,
			NewOut int,
            constraint Hosp_Casesmuniu foreign key(Id) references CasesMunicipality(Id) ON DELETE CASCADE
			)
               ''')

cursor.execute('''
		CREATE TABLE VaccinationDate (
			Id nvarchar(75) primary key,
            Datum Date,
			Region nvarchar(50),
			Count int
			)
               ''')

logging.info("Tabellen hosp, casesmunicipality en vaccinationdate zijn aangemaakt")

logging.info("Start met het vullen van de tabellen hosp, casesmunicipality en vaccinationdate")

conn.commit()

for i,row in df_CASES_MUNI.iterrows():
            sql = "INSERT INTO dbo.CasesMunicipality (Id, Date, Province, Region,Cases,KeyRegionDate) VALUES (?,?,?,?,?,?)"
            cursor.execute(sql, row['PK_CASES_MUNI'], row['DATE'],  row['PROVINCE'], row['REGION'], row['CASES'] ,row['PK_CASES_DATE_REGION']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            
conn.commit()

for i,row in df_HOSP.iterrows():
            sql = "INSERT INTO dbo.Hosp (Id ,Datum, Province, Region, NrReporting, TotalIn, TotalInIcu, TotalInResp, TotalInEcmo,NewIn,NewOut) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql, row["PK_HOSP"] ,row['DATE'], row['PROVINCE'], row['REGION'], row['NR_REPORTING'], row['TOTAL_IN'], row['TOTAL_IN_ICU'], row['TOTAL_IN_RESP'],row['TOTAL_IN_ECMO'],row['NEW_IN'],row['NEW_OUT']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            
conn.commit()

for i,row in df_VACC.iterrows():
            sql = "INSERT INTO dbo.VaccinationDate (Id ,Datum, Region, Count) VALUES (?,?,?,?)"
            cursor.execute(sql, row["PK_VACC_DATE_REGION"] ,row['DATE'], row['REGION'], row['COUNT']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            
conn.commit()

logging.info("Tabellen hosp, casesmunicipality en vaccinationdate zijn gevuld")


