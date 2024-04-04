import pandas as pd
import pyodbc

# -------------------------------- methodes ---------------------------------

def removeBrackets(potentielNumber):
    if "(" in str(potentielNumber):
        return int(potentielNumber[1:len(potentielNumber)-1])
    else:
        return potentielNumber

def makeRegionNameSimple(regionName):
    if regionName == "Brussels Hoofdstedelijk Gewest":
        return "Brussels"
    elif regionName == "Vlaams Gewest":
        return "Flanders"
    elif regionName == "Waals Gewest":
        return "Wallonia"

def deletePrefixProvince(provincie):
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

def makeAgeGroupWallonia(age):
    if age >= 0 and age <= 11:
        return '000 - 011'
    elif age >= 12 and age <= 17:
        return '012 - 017'
    elif age >= 18 and age <= 34:
        return '018 - 034'
    elif age >= 35 and age <= 44:
        return '035 - 044'
    elif age >= 45 and age <= 54:
        return '045 - 054'
    elif age >= 55 and age <= 64:
        return '055 - 064'
    elif age >= 65 and age <= 74:
        return '065 - 074'
    elif age >= 75 and age <= 84:
        return '075 - 084'
    else:
        return '085 et +'

def toString(niscode):
   return str(niscode)

def toInt(numberInString):
    return int(numberInString)

# -------------------------------- clean vaccination Wallonië --------------------------------


dfVaccinationWallonia = pd.read_csv("datasets_overzicht/Dataset_vaccinatieWallonië.csv")
dfVaccinationWallonia = dfVaccinationWallonia.dropna(how='all',axis='columns')


dfVaccinationWallonia = dfVaccinationWallonia.drop(columns=['Semaine','Nbre Pers Ayant Recu Au Moins1Dose'],axis=1)

dfVaccinationWallonia['Code INS Commune'] = dfVaccinationWallonia['Code INS Commune'].map(toString)

dfVaccinationWallonia['Nbre Pers Partiellement Vaccinées'] = dfVaccinationWallonia['Nbre Pers Partiellement Vaccinées'].map(removeBrackets).map(toInt)
dfVaccinationWallonia['Nbre Pers Totalement Vaccinées'] = dfVaccinationWallonia['Nbre Pers Totalement Vaccinées'].map(removeBrackets).map(toInt)

# -------------------------------- clean Population  --------------------------------

dfPopulation = pd.read_csv('./datasets_overzicht/Dataset_bevolking.csv')
dfPopulation = dfPopulation.dropna()



dfPopulation["TX_RGN_DESCR_NL"] = dfPopulation["TX_RGN_DESCR_NL"].map(makeRegionNameSimple)
dfPopulation["AgeGroupWal"] = dfPopulation["CD_AGE"].map(makeAgeGroupWallonia)
dfPopulation["CD_REFNIS"] = dfPopulation["CD_REFNIS"].map(toString)
dfPopulation["TX_PROV_DESCR_NL"] = dfPopulation["TX_PROV_DESCR_NL"].map(deletePrefixProvince)

dfPopulation = dfPopulation.drop(columns=["CD_AGE","CD_DSTR_REFNIS","TX_ADM_DSTR_DESCR_NL","TX_ADM_DSTR_DESCR_FR","CD_PROV_REFNIS","CD_RGN_REFNIS","CD_NATLTY","TX_NATLTY_NL","TX_NATLTY_FR","CD_CIV_STS","TX_CIV_STS_NL","TX_CIV_STS_FR"], axis="columns")

# --------------------------------  make PK --------------------------------

dfVaccinationWallonia['PK_VACCINATION_WALLONIA'] = dfVaccinationWallonia['Code INS Commune'] + dfVaccinationWallonia["Tranche d'âge"] + dfVaccinationWallonia["Genre"] + dfVaccinationWallonia['Commune'] + dfVaccinationWallonia['Province']
dfPopulation['PK_POPULATION'] = dfPopulation["CD_REFNIS"] + dfPopulation["AgeGroupWal"] + dfPopulation["CD_SEX"] + dfPopulation["TX_DESCR_NL"] + dfPopulation["TX_PROV_DESCR_NL"] 


# -------------------------------- Group VaccinationWallonia by  --------------------------------

dfVaccinationWallonia = dfVaccinationWallonia.groupby(by=["PK_VACCINATION_WALLONIA","Genre","Tranche d'âge","Code INS Commune","Commune","Province"]).sum().reset_index()

# -------------------------------- Group Population by  --------------------------------
dfPopulation = dfPopulation.groupby(by=["PK_POPULATION","CD_REFNIS","TX_DESCR_NL","TX_DESCR_FR","TX_PROV_DESCR_NL","TX_PROV_DESCR_FR","CD_SEX",'TX_RGN_DESCR_NL','TX_RGN_DESCR_FR',"AgeGroupWal"]).sum().reset_index()


# -------------------------------- Insert into DB --------------------------------

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS dbo.Population')
cursor.execute('DROP TABLE IF EXISTS dbo.VaccinationWallonia')
print("tables population and Vaccinatie dropped")


cursor.execute('''
		CREATE TABLE Population (
            Id nvarchar(100) primary key,
            RefnisCodeMunicipality int,
            MunicipalityNameNL nvarchar(50),
            ProvinceNameNL nvarchar(50),
            RegionName nvarchar(50),
            Sex nvarchar(50),
            Agegroup nvarchar(50),
            NoOfPeople int,
            Constraint FK_Casesmunicum Foreign key(RefnisCodeMunicipality) references CASES_MUNI_CUM(NIS)

			)
               ''')

# create the table VaccinationWallonia
cursor.execute('''
		CREATE TABLE VaccinationWallonia (
            id nvarchar(100) primary key,
			nisCode int ,
			gender nvarchar(50),
			ageGroup nvarchar(50),
            municipality nvarchar(50),
            province nvarchar(50),
            region nvarchar(50),
            fullyVaccinatedGeneral int,
            partlyVaccinatedGeneral int,
            constraint Casesmunicum_VaccinationWallonia_fk foreign key(nisCode) references CASES_MUNI_CUM(NIS),

			)
               ''')

conn.commit()
print("tables population and Vaccinatie created")


print("Start Record insert population")
for i,row in dfPopulation.iterrows():
            print(i)
            sql = "INSERT INTO dbo.Population(Id, RefnisCodeMunicipality, MunicipalityNameNL,  ProvinceNameNL, RegionName, Sex, Agegroup, NoOfPeople) VALUES (?,?,?,?,?,?,?,?)"
            cursor.execute(sql, row['PK_POPULATION'], row['CD_REFNIS'], row['TX_DESCR_NL'],  row['TX_PROV_DESCR_NL'], row['TX_RGN_DESCR_NL'], row['CD_SEX'], row['AgeGroupWal'], row['MS_POPULATION']).rowcount
            


print("Start Record insert for VaccinationWallonia")
for i,row in dfVaccinationWallonia.iterrows():
            print(i)
            sql = "INSERT INTO dbo.VaccinationWallonia (Id, nisCode, gender, ageGroup, municipality, province, region, fullyVaccinatedGeneral,  partlyVaccinatedGeneral) VALUES (?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql, row['PK_VACCINATION_WALLONIA'] ,row['Code INS Commune'], row['Genre'], row["Tranche d'âge"], row['Commune'], row['Province'], "Wallonia", row['Nbre Pers Totalement Vaccinées'] , row['Nbre Pers Partiellement Vaccinées']).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()

print("Record inserted for VaccinationWallonia")

# -------------------------------------- delete cases that doesn't match ------------------------------------------------

cursor.execute('''
delete from VaccinationWallonia
WHERE Id NOT IN
(SELECT Id from Population)


delete from Population
WHERE Id NOT IN
(SELECT Id from VaccinationWallonia)
''')

conn.commit()

# -------------------------------------- Make a relation with foreign key ------------------------------------------------

cursor.execute('''
ALTER TABLE [Population]
ADD CONSTRAINT FK_Population_VaccinationWallonia
FOREIGN KEY(Id)
REFERENCES VaccinationWallonia(Id)
ON DELETE CASCADE
''')

conn.commit()


