import pandas as pd
import pyodbc
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO)

logging.info('Inlezen datasets Cases_cum, cases_muni_cum_vla, bevolking en verbruik')

df_cases_cum = pd.read_csv(
    'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI_CUM.csv')
df_cases_cum = df_cases_cum.dropna(axis=0)

df_cases_muni_cum_vla = pd.read_csv(
    'https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI_CUM.csv')
df_cases_muni_cum_vla = df_cases_muni_cum_vla.dropna(axis=0)

df_bevolking = pd.read_excel(r'DATA\Dataset_bevolking.xlsx')
df_bevolking = df_bevolking.dropna(axis=0)

df_verbruik = pd.read_excel(r'DATA\Verbruik.xlsx')
df_verbruik = df_verbruik.dropna(axis=0)

logging.info('Datasets Cases_cum, cases_muni_cum_vla, bevolking en verbruik ingelezen')

logging.info('Cleanen van datasets Cases_cum, cases_muni_cum_vla, bevolking en verbruik')

df_verbruik['Hoofdgemeente'] = df_verbruik['Hoofdgemeente'].str.capitalize()
df_cases_muni_cum_vla = df_cases_muni_cum_vla[df_cases_muni_cum_vla.REGION == "Flanders"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Baarle-nassau"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Eigenbrakel"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Tubeke"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Aalst"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Beveren"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Halle"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Hamme"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Hove"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Kapellen"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Machelen"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Moerbeke"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Nieuwerkerken"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Sint-niklaas"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Aartselaar"]
df_verbruik = df_verbruik[df_verbruik.Hoofdgemeente != "Tielt"]

logging.info('Datasets Cases_cum, cases_muni_cum_vla, bevolking zijn gecleant')

logging.info('Maak connactie met de databank')

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=G6_DE;'
                      'Trusted_Connection=Yes;')
cursor = conn.cursor()

logging.info('Connectie succesvol')

logging.info('Tabellen Bevolking_belgie, verbruik, CasesPerGemeenteCUM en CasesPerGemeenteCUMVlaanderen droppen indien ze bestaan')

cursor.execute('DROP TABLE IF EXISTS dbo.Bevolking_Belgie')
cursor.execute('DROP TABLE IF EXISTS dbo.Verbruik')
cursor.execute('DROP TABLE IF EXISTS dbo.CasesPerGemeenteCUM')
cursor.execute('DROP TABLE IF EXISTS dbo.CasesPerGemeenteCUMVlaanderen')

logging.info('Tabellen Bevolking_belgie, verbruik, CasesPerGemeenteCUM en CasesPerGemeenteCUMVlaanderen gedropt')

logging.info('Tabellen Bevolking_belgie, verbruik, CasesPerGemeenteCUM en CasesPerGemeenteCUMVlaanderen aanmaken')

cursor.execute('''
	CREATE TABLE CasesPerGemeenteCUM (
        NIS5 int,
        TX_DESCR_NL nvarchar(50) primary key,
        TX_DESCR_FR nvarchar(50),
        TX_ADM_DSTR_DESCR_NL nvarchar(50),
        TX_ADM_DSTR_DESCR_FR nvarchar(50),
        PROVINCE nvarchar(50),
        REGION nvarchar(50),
        CASES int
		)
    ''')

cursor.execute('''
	CREATE TABLE CasesPerGemeenteCUMVlaanderen (
        NIS5 int,
        TX_DESCR_NL nvarchar(50) primary key,
        TX_DESCR_FR nvarchar(50),
        TX_ADM_DSTR_DESCR_NL nvarchar(50),
        TX_ADM_DSTR_DESCR_FR nvarchar(50),
        PROVINCE nvarchar(50),
        REGION nvarchar(50),
        CASES int
		)
    ''')

cursor.execute('''
	CREATE TABLE Bevolking_Belgie (
        identiteitBevolking int identity(1,1) primary key,
        CD_REFNIS int,
        TX_DESCR_NL nvarchar(50),
        TX_DESCR_FR nvarchar(50),
        CD_DSTR_REFNIS int,
        TX_ADM_DSTR_DESCR_NL nvarchar(50),
        TX_ADM_DSTR_DESCR_FR nvarchar(50),
        CD_PROV_REFNIS int,
        TX_PROV_DESCR_NL nvarchar(50),
        TX_PROV_DESCR_FR nvarchar(50),
        CD_RGN_REFNIS int,
        TX_RGN_DESCR_NL nvarchar(50),
        TX_RGN_DESCR_FR nvarchar(50),
        CD_SEX nvarchar(50),
        CD_NATLTY nvarchar(50),
        TX_NATLTY_NL nvarchar(50),
        TX_NATLTY_FR nvarchar(50),
        CD_CIV_STS int,
        TX_CIV_STS_NL nvarchar(50),
        TX_CIV_STS_FR nvarchar(50),
        CD_AGE int,
        MS_POPULATION int,
        Constraint FK_bevolking_casesmunicum Foreign key(TX_DESCR_NL) references CasesPerGemeenteCUM(TX_DESCR_NL) ON DELETE CASCADE
		)
    ''')

cursor.execute('''
	CREATE TABLE Verbruik (
        identiteitVerbruik int identity(1,1) primary key,
        Verbruiksjaar int,
        Hoofdgemeente nvarchar(50),
        Energie nvarchar(50),
        Injectie_Afname nvarchar(50),
        Straat nvarchar(50),
        Regio nvarchar(50),
        Aantal_toegangspunten int,
        Benaderend_Verbruik_kWh int,
        Constraint FK_verbruik_casesmunicum Foreign key(Hoofdgemeente) references CasesPerGemeenteCUMVlaanderen(TX_DESCR_NL) ON DELETE CASCADE
		)
    ''')

logging.info('Tabellen Bevolking_belgie, verbruik, CasesPerGemeenteCUM en CasesPerGemeenteCUMVlaanderen aangemaakt')

logging.info('Inlezen van datasets in tabellen')

for row in df_cases_cum.itertuples():
    cursor.execute('''
                INSERT INTO CasesPerGemeenteCUM (NIS5,  TX_DESCR_NL, TX_DESCR_FR, TX_ADM_DSTR_DESCR_NL, TX_ADM_DSTR_DESCR_FR, PROVINCE, REGION, CASES)VALUES (?,?,?,?,?,?,?,?)''',
                   row.NIS5,
                   row.TX_DESCR_NL,
                   row.TX_DESCR_FR,
                   row.TX_ADM_DSTR_DESCR_NL,
                   row.TX_ADM_DSTR_DESCR_FR,
                   row.PROVINCE,
                   row.REGION,
                   row.CASES
                   )
    
conn.commit()

for row in df_cases_muni_cum_vla.itertuples():
    cursor.execute('''
                INSERT INTO CasesPerGemeenteCUMVlaanderen (NIS5,  TX_DESCR_NL, TX_DESCR_FR, TX_ADM_DSTR_DESCR_NL, TX_ADM_DSTR_DESCR_FR, PROVINCE, REGION, CASES)VALUES (?,?,?,?,?,?,?,?)''',
                   row.NIS5,
                   row.TX_DESCR_NL,
                   row.TX_DESCR_FR,
                   row.TX_ADM_DSTR_DESCR_NL,
                   row.TX_ADM_DSTR_DESCR_FR,
                   row.PROVINCE,
                   row.REGION,
                   row.CASES
                   )
    
conn.commit()

for row in df_bevolking.itertuples():
    cursor.execute('''
                INSERT INTO Bevolking_Belgie (CD_REFNIS, TX_DESCR_NL, TX_DESCR_FR, CD_DSTR_REFNIS, TX_ADM_DSTR_DESCR_NL, TX_ADM_DSTR_DESCR_FR, CD_PROV_REFNIS, TX_PROV_DESCR_NL,	TX_PROV_DESCR_FR,	CD_RGN_REFNIS,	TX_RGN_DESCR_NL,	TX_RGN_DESCR_FR,	CD_SEX,	CD_NATLTY,	TX_NATLTY_NL,	TX_NATLTY_FR,	CD_CIV_STS,	TX_CIV_STS_NL,	TX_CIV_STS_FR,	CD_AGE,MS_POPULATION)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   row.CD_REFNIS,
                   row.TX_DESCR_NL,
                   row.TX_DESCR_FR,
                   row.CD_DSTR_REFNIS,
                   row.TX_ADM_DSTR_DESCR_NL,
                   row.TX_ADM_DSTR_DESCR_FR,
                   row.CD_PROV_REFNIS,
                   row.TX_PROV_DESCR_NL,
                   row.TX_PROV_DESCR_FR,
                   row.CD_RGN_REFNIS,
                   row.TX_RGN_DESCR_NL,
                   row.TX_RGN_DESCR_FR,
                   row.CD_SEX,
                   row.CD_NATLTY,
                   row.TX_NATLTY_NL,
                   row.TX_NATLTY_FR,
                   row.CD_CIV_STS,
                   row.TX_CIV_STS_NL,
                   row.TX_CIV_STS_FR,
                   row.CD_AGE,
                   row.MS_POPULATION
                   )
    
conn.commit()

for i, row in df_verbruik.iterrows():
    sql = "INSERT INTO dbo.Verbruik(Verbruiksjaar, Hoofdgemeente,  Energie, Injectie_Afname, Straat, Regio, Aantal_toegangspunten, Benaderend_Verbruik_kWh) VALUES (?,?,?,?,?,?,?,?)"
    cursor.execute(sql, row['Verbruiksjaar'], row['Hoofdgemeente'],  row['Energie'],
                   row['Injectie_Afname'], row['Straat'], row['Regio'], row['Aantal_toegangspunten'], row['Benaderend_Verbruik_kWh']).rowcount
conn.commit()

logging.info("Datasets zijn ingelezen")