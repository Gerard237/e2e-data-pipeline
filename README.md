# e2e-data-pipeline

## A. Source des données : data.gouv.fr

## B. Description des données

Ces données donne l'evolution de la consomation d'electricite et gaz de 2011 à 2023 par adresse, commune, region, secteur activité, catégorie de consommation, par code NAF et niveau nationale.

Lien vers les differentes source de donnée : 

1- Conso annuelle électricité et gaz par adresse : https://data.enedis.fr/explore/dataset/consommation-annuelle-residentielle-par-adresse/api/

2- Conso annuelle électricité et gaz par commune : https://opendata.agenceore.fr/explore/dataset/consommation-annuelle-d-electricite-et-gaz-par-commune/api/?disjunctive.nom_commune&disjunctive.nom_epci&disjunctive.nom_departement&disjunctive.nom_region

3- Conso annuelle électricité et gaz par département : https://opendata.agenceore.fr/explore/dataset/consommation-annuelle-d-electricite-et-gaz-par-departement/api/?disjunctive.operateur&disjunctive.filiere&disjunctive.code_categorie_consommation&disjunctive.code_grand_secteur

4- Conso annuelle électricité et gaz par national : https://odre.opendatasoft.com/explore/dataset/consommation-annuelle-brute/information/

5- Conso annuelle électricité et gaz par régionale : https://odre.opendatasoft.com/explore/dataset/consommation-annuelle-brute-regionale/information/?disjunctive.region

6- Conso quotidien brut nationale : https://odre.opendatasoft.com/explore/dataset/consommation-quotidienne-brute/api/?sort=-date_heure

7- Conso quotidien brut régional : https://odre.opendatasoft.com/explore/dataset/consommation-quotidienne-brute-regionale/information/?disjunctive.region&disjunctive.code_insee_region

## C. Prérequis

- Airflow 2.10.3
- Spark standalone 3.5.3
- Python 3.8.16
- PySpark 3.5.4
- Compte sur AWS et a bucket S3, access key et secret key for an IAM user avec un role d'ecriture sur S3
- Download les libriarie : aws-java-sdk-bundle-1.11.874.jar, hadoop-aws-3.3.4.jar et le mettre dans le dossier jars de spark et de pyspark

## D. Description de l'architecture

- Nous avons recuperer les données à travers une API qui provient des liens vers le source de donnée ci-dessous.
- Traitement de la donnée avec PySpark et execution du job sur un Spark Standalone with hadoop integrated.
- Stockage des données traiter un fichier csv sur bucket s3.
- Visualisation des données avec l'outil tableau : pour ressortir : consommation totale annuelle et moyenne annuelle par adresse, commune, secteur d'activité, region et niveau national.

![alt text](https://github.com/Gerard237/e2e-data-pipeline/blob/main/logo.drawio.png)

## E. Resultat

Nous avons un tableau de bord sur Tableau des doonéés : consommation totale annuelle et moyenne annuelle par adresse, commune, region

![alt text](https://github.com/Gerard237/e2e-data-pipeline/blob/main/conso_electricite_gaz.png)

