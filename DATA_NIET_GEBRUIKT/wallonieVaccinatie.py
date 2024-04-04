import pandas as pd
import pyodbc

wallonieVaccinatie = pd.read_csv("datasets_overzicht/Dataset_vaccinatieWallonië.csv")

# this code will remove all the columns where all the elements are missing (Usefull in this case!)
wallonieVaccinatie = wallonieVaccinatie.dropna(how='all',axis='columns')

# clean features about amount vaccinated, it contains the value in breakets so is interpreted in string
def removeBrackets(potentielNumber):
    if "(" in str(potentielNumber):
        return int(potentielNumber[1:len(potentielNumber)-1])
    else:
        return potentielNumber

wallonieVaccinatie['Nbre Pers Partiellement Vaccinées'] = wallonieVaccinatie['Nbre Pers Partiellement Vaccinées'].map(removeBrackets)
wallonieVaccinatie['Nbre Pers Totalement Vaccinées'] = wallonieVaccinatie['Nbre Pers Totalement Vaccinées'].map(removeBrackets)
wallonieVaccinatie['Nbre Pers Ayant Recu Au Moins1Dose'] = wallonieVaccinatie['Nbre Pers Ayant Recu Au Moins1Dose'].map(removeBrackets)


# add the column region that can be usefull for relation by region
wallonieVaccinatie["Region"] = "Wallonie"
wallonieVaccinatie["RegionEn"] = "Wallonia"

# overview dataset
print(wallonieVaccinatie)

# making connection with database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')

print("Connection successful")

# drop the table if it already exists
cursor = conn.cursor()
cursor.execute('DROP TABLE IF EXISTS dbo.VaccinatieWallonieGemeente')

# create the table VaccinatieWallonieGemeente
cursor.execute('''
		CREATE TABLE VaccinatieWallonieGemeente (
            id int identity(1,1) primary key,
            week nvarchar(50),
			nisCode int ,
			gender nvarchar(50),
			ageGroup nvarchar(50),
            municipality nvarchar(50),
            province nvarchar(50),
            region nvarchar(50),
            fullyVaccinatedGeneral int,
            partlyVaccinatedGeneral int,
            onceVaccinatedGeneral int,
            nrPopulation int
			)
               ''')


conn.commit()
print("Start Record insert")
for i,row in wallonieVaccinatie.iterrows():
            sql = "INSERT INTO dbo.VaccinatieWallonieGemeente (week,nisCode, gender, ageGroup, municipality, province, region, fullyVaccinatedGeneral,  partlyVaccinatedGeneral, onceVaccinatedGeneral, nrPopulation) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql, row['Semaine'] ,row['Code INS Commune'], row['Genre'], row["Tranche d'âge"], row['Commune'], row['Province'], row['RegionEn'], row['Nbre Pers Totalement Vaccinées'] , row['Nbre Pers Partiellement Vaccinées'], row['Nbre Pers Ayant Recu Au Moins1Dose'], 0).rowcount
            # the connection is not auto committed by default, so we must commit to save our changes
            print(f"{i}de row inserted")
            conn.commit()

print("Record inserted")



