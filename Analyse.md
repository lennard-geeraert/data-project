
# Potentiele API's / datasets

## ERD

![ERD Data Engineer Project](https://user-images.githubusercontent.com/43605003/136527577-7245cf0c-b4b3-4cd8-a83a-1d1a6e06646c.png)
https://lucid.app/lucidchart/b9f58593-44dc-4594-92ff-362630cc54d5/edit?viewport_loc=16%2C48%2C1910%2C785%2C0_0&invitationId=inv_8f612cdd-8c76-44ba-a9c6-256925d8b53e

## Api's die we gaan gebruiken
https://epistat.wiv-isp.be/covid/  
https://www.laatjevaccineren.be/vaccinatieteller-cijfers-per-gemeente
https://statbel.fgov.be/en/open-data

https://github.com/Thib-G/coronavirus/tree/master/data (script voor inspiratie)
https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/ZwmOB (idee voor visualisatie)

## Onderzoeksvragen
- Is er een verband tussen de vaccinatiegraad in een gemeente en het gemiddelde opleidingsniveau?
- Is er een verband tussen de vaccinatiegraad in een gemeente tegenover de religies?
- Is er een verband tussen de vaccinatiegraad in een gemeente en het geslacht?
- Is er een verband tussen de vaccinatiegraad in een gemeente en politieke voorkeur?
- Is er een verband tussen de vaccinatiegraad en de leeftijdsgroepen?

## onderzoeksvragen idee
- Is er een verband tussen de vaccinatiegraad in een gemeente en soort prefered vaccin(AstraZeneca/Physer ...)?

## Teo

https://epistat.wiv-isp.be/covid/  
https://github.com/Thib-G/coronavirus/tree/master/data --> deze kerel kopieert de data van de bovenstaande link elk uur naar deze repo  
https://bestat.statbel.fgov.be/bestat/api/views/ --> lijkt me niet echt bruikbaar  
https://statbel.fgov.be/nl/open-data --> heel veel verschillende datasets over verscheidene onderwerpen  

## Elias
#### Dataset 1: https://epistat.wiv-isp.be/covid/
##### naam: vacc
###### Kolommen
- Datum
- Regio
- Leeftijdsgroep
- Gender
- Merk vaccin (vb. J&J)
- Dosis (A = eerste dosis, B = tweede dosis, c = vaccin met maar 1 dosis, e = derde dosis)
- aantal

- Basisvisualisatie vaccinatie: https://datastudio.google.com/embed/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB

#### Dataset 2: https://statbel.fgov.be/en/open-data
##### naam: Population by place of residence, nationality, marital status, age and sex
- Handig om deze dataset te koppelen aan bovenstaande voor vaccinatiegraad / geografisch gebied
###### Kolommen (veel)
- Naam van de gemeente in NL, Naam van de gemeente in FR, Refnis-code van het arrondissement, Naam van het arrondissement in NL, Naam van het arrondissement in FR
- Refnis-code van de provincie, Naam van de provincie in NL, Naam van de provincie in FR, Refnis-code van het gewest, Naam van het gewest in NL
- Naam van het gewest in FR, Geslacht, Nationaliteitscode, Nationaliteit in NL, Nationaliteit in FR, Code burgerlijke staat, Burgerlijke staat in NL
- Burgerlijke staat in FR, Leeftijd, Aantal personen, Referentiejaar

#### Datasets 3: https://data.gov.be
##### Alle open data van de belgische overheid
###### Topics

#### Datasets 4: https://overheid.vlaanderen.be/informatie-vlaanderen/ontdek-onze-producten-en-diensten/open-data-bij-de-vlaamse-overheid
- Lijkt minder interessant
- Economy and Finance **(kijken of er een gemiddeld inkomen kan bekomen worden)**
- Public Sector
- Environment
- Regional
- Population **(handig voor aantal inwoners per x)**
- Science and Technology
- Health
- International
- Culture and Sports
- Energy
- Agriculture and Fisheries
- Transport

#### Datasets 4: https://overheid.vlaanderen.be/informatie-vlaanderen/ontdek-onze-producten-en-diensten/open-data-bij-de-vlaamse-overheid
- Lijkt minder interessant

#### extra
- uitlsagen verkiezingen 2018 (gemeente): https://www.vlaanderenkiest.be/verkiezingen2018/#/

## Mamadou 
paar onderzoeken verrichten

Is er een verband tussen de vaccinatiegraad in een gemeente en het gemiddelde inkomen?
Is er een verband tussen de vaccinatiegraad in een gemeente en het gemiddelde opleidingsniveau?
Is er een verband tussen de vaccinatiegraad in een gemeente tegenover de religies?
		Vaccineren mensen die katholiek zijn zich meer dan mensen die moslim of jood zijn?
Is er een verband tussen de vaccinatiegraad in een gemeente en mensen hun werkstatus (werkloos, of werkzaam)?
Is er een verband tussen de vaccinatiegraad in een gemeente en politieke voorkeur?
Is er een verband tussen de vaccinatiegraad in een gemeente en sterfte graad?

gemeente kan ook vervangen worden door provincie of streek of vlaanderen en wallonië?
mogen we ook andere onderzoeksvragen bv iets vergelijken zonder vaccinatiegraad

api's
idee voor visualisering:
https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/ZwmOB
zelf gezocht:
https://data.opendatasoft.com/explore/dataset/covid-19-pandemic-belgium-deaths-agesexdate%40public/table/?disjunctive.region&disjunctive.agegroup&disjunctive.sex&sort=date
  https://statbel.fgov.be/en/open-data
https://www.lecho.be/dossiers/coronavirus/ou-en-est-la-campagne-de-vaccination-contre-le-covid-19-en-belgique/10281818.html
https://www.laatjevaccineren.be/vaccinatieteller-cijfers-per-gemeente

