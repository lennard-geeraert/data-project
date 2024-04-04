# -------------------------------- imports ---------------------------------
import pandas as pd
import numpy as np
import pyodbc
import requests as rq
import csv
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

logging.info('methodes voor het cleanen van data voorbereiden')

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

def makeRegionNameSimple(regionName):
    if regionName == "Brussels Hoofdstedelijk Gewest":
        return "Brussels"
    elif regionName == "Vlaams Gewest":
        return "Flanders"
    elif regionName == "Waals Gewest":
        return "Wallonia"

def deletePrefixProvince(provincie):
    if provincie == "nan":
     return ""
    provinieNaamZonderPrefix =  provincie.split(" ")[1]
    return provinieNaamZonderPrefix

def makeAgeGroup(age):
    if age >= 0 and age <= 11:
        return '0-11'
    elif age >= 12 and age <= 17:
        return '12-17'
    elif age >= 18 and age <= 29:
        return '18-29'
    elif age >= 30 and age <= 39:
        return '30-39'
    elif age >= 40 and age <= 49:
        return '40-49'
    elif age >= 50 and age <= 59:
        return '50-59'
    elif age >= 60 and age <= 69:
        return '60-69'
    elif age >= 70 and age <= 79:
        return '70-79'
    elif age >= 80 and age <= 89:
        return '80-89'
    else:
        return '90-100+'

logging.info('Methodes zijn gemaakt')

logging.info("Data vaccinatieRegio, bevolking en CasesRegio inlezen en cleanen met opgebouwde methodes")

# -------------------------------- clean VaccinatieSeansanoRegio's --------------------------------   
# Verschil tussen vaccintatiegraad per regio
# dus vaccinatiegraad per regio

vaccinatiesRegioDataset = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_VACC.csv')

# regio agegroup and sex nodig

vaccinatiesRegioDataset = vaccinatiesRegioDataset.dropna(axis=0)

# -------------------------------- clean LAATJEVACCINEREN --------------------------------        

URL = "https://www.laatjevaccineren.be/vaccination-info/get"
vaccinatiesDataset = None

with rq.Session() as s:
    download = s.get(URL)

    decoded_content = download.content.decode("utf-8")

    cr = csv.reader(decoded_content.splitlines(), delimiter=",")
    csv_in_listOfarray = list(cr)

    filtedFromColumnArray = np.array(csv_in_listOfarray[1:])

    vaccinatiesDataset = pd.DataFrame(data=np.array(filtedFromColumnArray),columns=csv_in_listOfarray[0])
    
    vaccinatiesDataset = vaccinatiesDataset.dropna()
    vaccinatiesDataset = vaccinatiesDataset.drop(columns=["ADULT_FL(18+)","SENIOR_FL(65+)","EERSTELIJNSZONE"])
    
    vaccinatiesDataset  = vaccinatiesDataset.replace({'V': 'F'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'WEST-VLAANDEREN' : 'West-Vlaanderen'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'OOST-VLAANDEREN' : 'Oost-Vlaanderen'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'ANTWERPEN' : 'Antwerpen'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'LIMBURG' : 'Limburg'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'VLAAMS-BRABANT' : 'Vlaams-Brabant'})
    vaccinatiesDataset = vaccinatiesDataset.replace({'VLAAMS GEWEST' : 'Flanders'})
    

    vaccinatiesDataset['PK_VACCINATIEGEMEENTE'] = vaccinatiesDataset['NIS_CD'] +  vaccinatiesDataset['GENDER_CD'] +  vaccinatiesDataset['AGE_CD'] + vaccinatiesDataset['MUNICIPALITY']+ vaccinatiesDataset['PROVINCE']+ vaccinatiesDataset['REGION']
   

# -------------------------------- clean CASES_MUNI --------------------------------   

df_CASES_MUNI_CUM = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI_CUM.csv')

df_CASES_MUNI_CUM = pd.DataFrame(df_CASES_MUNI_CUM)

df_CASES_MUNI_CUM = df_CASES_MUNI_CUM.dropna()
df_CASES_MUNI_CUM = df_CASES_MUNI_CUM.drop(columns=['TX_ADM_DSTR_DESCR_NL','TX_ADM_DSTR_DESCR_FR'])
df_CASES_MUNI_CUM['CASES'] = df_CASES_MUNI_CUM['CASES'].map(filterCases)


# -------------------------------- clean bevolking --------------------------------

bevolkingDataset = pd.read_csv('./DATA/Dataset_bevolking.csv')
bevolkingDataset["TX_PROV_DESCR_NL"] = bevolkingDataset["TX_PROV_DESCR_NL"].map(toString)
bevolkingDataset["TX_RGN_DESCR_NL"] = bevolkingDataset["TX_RGN_DESCR_NL"].map(makeRegionNameSimple)
bevolkingDataset["AgegroupVlaanderen"] = bevolkingDataset["CD_AGE"].map(makeAgeGroup)
bevolkingDataset["CD_REFNIS"] = bevolkingDataset["CD_REFNIS"].map(toString)
bevolkingDataset["TX_PROV_DESCR_NL"] = bevolkingDataset["TX_PROV_DESCR_NL"].map(deletePrefixProvince)

bevolkingDataset["PK_Bevolking"] = bevolkingDataset["CD_REFNIS"] + bevolkingDataset["CD_SEX"] + bevolkingDataset["AgegroupVlaanderen"] + bevolkingDataset["TX_DESCR_NL"] + bevolkingDataset["TX_PROV_DESCR_NL"] 

bevolkingDataset = bevolkingDataset.drop(columns=["CD_AGE","CD_DSTR_REFNIS","TX_ADM_DSTR_DESCR_NL","TX_ADM_DSTR_DESCR_FR","CD_PROV_REFNIS","CD_RGN_REFNIS","CD_NATLTY","TX_NATLTY_NL","TX_NATLTY_FR","CD_CIV_STS","TX_CIV_STS_NL","TX_CIV_STS_FR"], axis="columns")

logging.info('Datasets bevolking, vacc en cases ingelzen en gecleant')

# -------------------------------- Insert into DB --------------------------------

logging.info('Proberen connectie te maken met de databank')

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')

logging.info('Connectie succesvol')

cursor = conn.cursor()

logging.info('Verwijderen van Tabellen VaccinatieRegio, Population, CasesMUNI_CUM en VaccinatieGemeente')

cursor.execute('DROP TABLE IF EXISTS dbo.VaccinatieRegio')
cursor.execute('DROP TABLE IF EXISTS dbo.Population')
cursor.execute('DROP TABLE IF EXISTS dbo.VaccinatieGemeente')

cursor.execute('DROP TABLE IF EXISTS dbo.CASES_MUNI_CUM')

logging.info("Tabellen zijn verwijderd")

logging.info("Tabellen VaccinatieRegio, Population, CasesMUNI_CUM en  VaccinatieGemeente aanmaken")

cursor.execute('''
		CREATE TABLE CASES_MUNI_CUM (
	    NIS int primary key,
        MUNICIPALITYNL nvarchar(50) ,
        MUNICIPALITYFR nvarchar(50) ,
        PROVINCE nvarchar(50),	
        REGION	nvarchar(50),
        CASES int
        )
			
    ''')
cursor.execute('''
		CREATE TABLE VaccinatieGemeente (
            primaryKeyVaccinatieGemeente int identity(1,1) primary key,
            nisCode int,
			gender nvarchar(50),
			ageGroup nvarchar(50),
            municipality nvarchar(50),
            province nvarchar(50),
            region nvarchar(50),
            fullyVaccinatedGeneral int,
            partlyVaccinatedGeneral int,
            fullyVaccinatedAstra int,
            partlyVaccinatedAstra int,
            fullyVaccinatedPfizer int,
            partlyVaccinatedPfizer int,
            fullyVaccinatedModerna int,
            partlyVaccinatedModerna int,
            fullyVaccinatedJohnson int,
            parlyVaccinatedOther int,
            nrPopulation int,
            constraint casesmunicum_fk foreign key(nisCode) references CASES_MUNI_CUM(NIS) ON DELETE CASCADE
			)''')

cursor.execute('''
		CREATE TABLE Population (
            PopulationPk int identity(1,1) primary key,
            RefnisCodeMunicipality int,
            MunicipalityNameNL nvarchar(50),
            ProvinceNameNL nvarchar(50),
            RegionName nvarchar(50),
            Sex nvarchar(50),
            Agegroup nvarchar(50),
            NoOfPeople int,
            constraint FK_Casesmunicum Foreign key(RefnisCodeMunicipality) references CASES_MUNI_CUM(NIS) ON DELETE CASCADE
			)
               ''')

cursor.execute('''
		CREATE TABLE VaccinatieRegio (
                id int identity(1,1) primary key,
                region nvarchar(50),
                ageGroup nvarchar(50),
                sex nvarchar(50),
                date Date,
                brand nvarchar(50),
                dose char,
                count int
			)
               ''')

logging.info("Tabellen VaccinatieRegio, Population, CasesMUNI_CUM en  VaccinatieGemeente aagemaakt")

logging.info("Tabellen VaccinatieRegio, Population, CasesMUNI_CUM en  VaccinatieGemeente inlezen")

conn.commit()

for row in df_CASES_MUNI_CUM.itertuples():
    cursor.execute('''
                INSERT INTO CASES_MUNI_CUM (NIS,  MUNICIPALITYNL, MUNICIPALITYFR,PROVINCE, REGION, CASES)VALUES (?,?,?,?,?,?)''',
                   row.NIS5,
                   row.TX_DESCR_NL,
                   row.TX_DESCR_FR,
                   row.PROVINCE,
                   row.REGION,
                   row.CASES
                   
                   )

conn.commit()
    
for i,row in vaccinatiesDataset.iterrows():
            sql = "INSERT INTO dbo.VaccinatieGemeente (nisCode, gender, ageGroup, municipality, province,region, fullyVaccinatedGeneral,  partlyVaccinatedGeneral,fullyVaccinatedAstra, partlyVaccinatedAstra,  fullyVaccinatedPfizer, partlyVaccinatedPfizer,fullyVaccinatedModerna, partlyVaccinatedModerna,fullyVaccinatedJohnson,  parlyVaccinatedOther,nrPopulation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql, row['NIS_CD'], row['GENDER_CD'], row['AGE_CD'], row['MUNICIPALITY'], row['PROVINCE'], row['REGION'],  row['FULLY_VACCINATED_AMT'] , row['PARTLY_VACCINATED_AMT'], row['FULLY_VACCINATED_AZ_AMT'], row['PARTLY_VACCINATED_AZ_AMT'],row['FULLY_VACCINATED_PF_AMT'],row['PARTLY_VACCINATED_PF_AMT'],row['FULLY_VACCINATED_MO_AMT'],row['PARTLY_VACCINATED_MO_AMT'],row['FULLY_VACCINATED_JJ_AMT'],row['PARTLY_VACCINATED_OTHER_AMT'],row['POPULATION_NBR']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            
conn.commit()

for i,row in bevolkingDataset.iterrows():
            sql = "INSERT INTO dbo.Population(RefnisCodeMunicipality, MunicipalityNameNL,  ProvinceNameNL, RegionName, Sex, Agegroup, NoOfPeople) VALUES (?,?,?,?,?,?,?)"
            cursor.execute(sql,row['CD_REFNIS'], row['TX_DESCR_NL'],  row['TX_PROV_DESCR_NL'], row['TX_RGN_DESCR_NL'], row['CD_SEX'], row['AgegroupVlaanderen'], row['MS_POPULATION']).rowcount
            
conn.commit()

for i,row in vaccinatiesRegioDataset.iterrows():
            sql = "INSERT INTO dbo.VaccinatieRegio (region, ageGroup, sex, date, brand, dose, count) VALUES (?,?,?,?,?,?,?)"
            cursor.execute(sql, row['REGION'], row['AGEGROUP'], row['SEX'], row['DATE'], row['BRAND'], row['DOSE'], row['COUNT']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            
conn.commit()

logging.info("Tabellen VaccinatieRegio, Population, CasesMUNI_CUM en  VaccinatieGemeente ingelezen")

