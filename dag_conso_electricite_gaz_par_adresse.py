import json
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import pandas as pd
import logging

default_args = {
    'owner': 'gerard',
    'start_date': datetime(2024, 1, 10, 00, 00)
}


def get_data_conso_electricite_gaz_adresse(data,limit, offset):
    print("get_data_conso_electricite_gaz_adresse offset = ", offset)

    if offset == 9999:
        limit=1

    url = f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?select=annee%2C%20code_iris%2C%20nom_iris%2C%20numero_de_voie%2C%20indice_de_repetition%2C%20type_de_voie%2C%20libelle_de_voie%2C%20code_commune%2C%20nom_commune%2C%20segment_de_client%2C%20nombre_de_logements%2C%20consommation_annuelle_totale_de_l_adresse_mwh%2C%20consommation_annuelle_moyenne_par_site_de_l_adresse_mwh%2C%20consommation_annuelle_moyenne_de_la_commune_mwh%2C%20code_epci%2C%20code_departement%2C%20code_region&limit={limit}&offset={offset}&refine=annee%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    for tmp in res["results"]:
        data.append(tmp)
    
    if offset < 9999:
        offset+=101
        get_data_conso_electricite_gaz_adresse(data = data,limit = limit,offset = offset)

    return data

def get_data_conso_electricite_gaz_commune(data,limit, offset):
    print("get_data_conso_electricite_gaz_commune offset = ", offset)

    if offset == 9999:
        limit=1

    url = f"https://opendata.agenceore.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-d-electricite-et-gaz-par-commune/records?select=operateur%2C%20%20%20%20%20filiere%2C%20%20%20%20%20annee%2C%20%20%20%20%20code_commune%2C%20%20%20%20%20nom_commune%2C%20%20%20%20%20code_epci%2C%20%20%20%20%20nom_epci%2C%20%20%20%20%20type_epci%2C%20%20%20%20%20code_departement%2C%20%20%20%20%20nom_departement%2C%20%20%20%20%20code_region%2C%20%20%20%20%20nom_region%2C%20%20%20%20%20code_categorie_consommation%2C%20%20%20%20%20code_grand_secteur%2C%20%20%20%20%20code_secteur_naf2%2C%20%20%20%20%20nb_sites%2C%20%20%20%20%20conso_totale_mwh%2C%20%20%20%20%20conso_moyenne_mwh%2C%20%20%20%20%20nombre_d_habitants%2C%20%20%20%20%20taux_de_logements_collectifs%2C%20%20%20%20%20taux_de_residences_principales%2C%20%20%20%20%20superficie_des_logements_30_m2%2C%20%20%20%20%20superficie_des_logements_30_a_40_m2%2C%20%20%20%20%20superficie_des_logements_40_a_60_m2%2C%20%20%20%20%20superficie_des_logements_60_a_80_m2%2C%20%20%20%20%20superficie_des_logements_80_a_100_m2%2C%20%20%20%20%20superficie_des_logements_100_m2%2C%20%20%20%20%20residences_principales_avant_1919%2C%20%20%20%20%20residences_principales_de_1919_a_1945%2C%20%20%20%20%20residences_principales_de_1946_a_1970%2C%20%20%20%20%20residences_principales_de_1971_a_1990%2C%20%20%20%20%20residences_principales_de_1991_a_2005%2C%20%20%20%20%20residences_principales_de_2006_a_2015%2C%20%20%20%20%20residences_principales_apres_2016%2C%20%20%20%20%20taux_de_chauffage_electrique&limit={limit}&offset={offset}&refine=annee%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    for tmp in res["results"]:
        data.append(tmp)
    
    if offset < 9999:
        offset+=101
        get_data_conso_electricite_gaz_commune(data = data,limit = limit,offset = offset)

    return data

def get_data_conso_electricite_gaz_departement(data,limit, offset):
    print("get_data_conso_electricite_gaz_departement offset = ", offset)

    if offset == 9999:
        limit=1

    url = f"https://opendata.agenceore.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-d-electricite-et-gaz-par-departement/records?select=operateur%2C%20%20%20%20%20filiere%2C%20%20%20%20%20annee%2C%20%20%20%20%20code_departement%2C%20%20%20%20%20nom_departement%2C%20%20%20%20%20code_region%2C%20%20%20%20%20nom_region%2C%20%20%20%20%20code_categorie_consommation%2C%20%20%20%20%20code_grand_secteur%2C%20%20%20%20%20code_secteur_naf2%2C%20%20%20%20%20nb_sites%2C%20%20%20%20%20conso_totale_mwh%2C%20%20%20%20%20conso_moyenne_mwh%2C%20%20%20%20%20nombre_d_habitants%2C%20%20%20%20%20taux_de_logements_collectifs%2C%20%20%20%20%20taux_de_residences_principales%2C%20%20%20%20%20superficie_des_logements_30_m2%2C%20%20%20%20%20superficie_des_logements_30_a_40_m2%2C%20%20%20%20%20superficie_des_logements_40_a_60_m2%2C%20%20%20%20%20superficie_des_logements_60_a_80_m2%2C%20%20%20%20%20superficie_des_logements_80_a_100_m2%2C%20%20%20%20%20superficie_des_logements_100_m2%2C%20%20%20%20%20residences_principales_avant_1919%2C%20%20%20%20%20residences_principales_de_1919_a_1945%2C%20%20%20%20%20residences_principales_de_1946_a_1970%2C%20%20%20%20%20residences_principales_de_1971_a_1990%2C%20%20%20%20%20residences_principales_de_1991_a_2005%2C%20%20%20%20%20residences_principales_de_2006_a_2015%2C%20%20%20%20%20residences_principales_apres_2016%2C%20%20%20%20%20taux_de_chauffage_electrique&limit={limit}&offset={offset}&refine=annee%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    for tmp in res["results"]:
        data.append(tmp)
    
    if offset < 9999:
        offset+=101
        get_data_conso_electricite_gaz_departement(data = data,limit = limit,offset = offset)

    return data

def get_data_conso_electricite_gaz_nationale():
    
    url = f"https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-annuelle-brute/records?limit=20&refine=annee%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    return res["results"]

def get_data_conso_electricite_gaz_regionale():
    
    url = f"https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-annuelle-brute-regionale/records?limit=20&refine=annee%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    return res["results"]

def get_data_conso_quotidien_electricite_gaz_regionale(data,limit, offset):
    print("get_data_conso_quotidien_electricite_gaz_regionale offset = ", offset)

    if offset == 9999:
        limit=1

    url = f"https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/records?select=date_heure%2C%20%20%20%20%20code_insee_region%2C%20%20%20%20%20region%2C%20%20%20%20%20consommation_brute_gaz_totale%2C%20%20%20%20%20consommation_brute_electricite_rte%2C%20%20%20%20%20consommation_brute_totale%2C%20%20%20%20%20flag_ignore&limit={limit}&offset={offset}&refine=date_heure%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    for tmp in res["results"]:
        data.append(tmp)
    
    if offset < 9999:
        offset+=101
        get_data_conso_quotidien_electricite_gaz_regionale(data = data,limit = limit,offset = offset)

    return data

def get_data_conso_quotidien_electricite_gaz_nationale(data,limit, offset):
    print("get_data_conso_quotidien_electricite_gaz_nationale offset = ", offset)

    if offset == 9999:
        limit=1

    url = f"https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute/records?select=date_heure%2Cconsommation_brute_gaz_totale%2Cconsommation_brute_electricite_rte%2Cconsommation_brute_totale&limit={limit}&offset={offset}&refine=date_heure%3A%222023%22"
    res = requests.get(url=url)
    res = res.json()

    for tmp in res["results"]:
        data.append(tmp)
    
    if offset < 9999:
        offset+=101
        get_data_conso_quotidien_electricite_gaz_nationale(data = data,limit = limit,offset = offset)

    return data

def api_call():

    try:
        #conso_electricite_gaz_adresse
        conso_electricite_gaz_adresse = get_data_conso_electricite_gaz_adresse(data=[],limit=100, offset=0)

        df = pd.DataFrame(data=conso_electricite_gaz_adresse)
        df.to_csv("conso_annuelle_electricite_gaz_adresse.csv",index=False)

        #conso_electricite_gaz_commune
        conso_electricite_gaz_commune = get_data_conso_electricite_gaz_commune(data=[],limit=100, offset=0)

        df = pd.DataFrame(data=conso_electricite_gaz_commune)
        df.to_csv("../data/conso_annuelle_electricite_gaz_commune.csv",index=False)

        # conso_electricite_gaz_departement
        conso_electricite_gaz_departement = get_data_conso_electricite_gaz_departement(data=[],limit=100, offset=0)

        df = pd.DataFrame(data=conso_electricite_gaz_departement)
        df.to_csv("../data/conso_annuelle_electricite_gaz_departement.csv",index=False)

        # conso_electricite_gaz_nationale
        conso_electricite_gaz_nationale = get_data_conso_electricite_gaz_nationale()

        df = pd.DataFrame(data=conso_electricite_gaz_nationale)
        df.to_csv("../data/conso_annuelle_electricite_gaz_nationale.csv",index=False)

        # conso_electricite_gaz_regionale
        conso_electricite_gaz_regionale = get_data_conso_electricite_gaz_regionale()

        df = pd.DataFrame(data=conso_electricite_gaz_regionale)
        df.to_csv("../data/conso_annuelle_electricite_gaz_regionale.csv",index=False)

        # conso_quotidien_electricite_gaz_regionale
        conso_quotidien_electricite_gaz_regionale = get_data_conso_quotidien_electricite_gaz_regionale(data=[],limit=100, offset=0)

        df = pd.DataFrame(data=conso_quotidien_electricite_gaz_regionale)
        df.to_csv("../data/conso_quotidien_electricite_gaz_regionale.csv",index=False)

        # conso_quotidien_electricite_gaz_nationale
        conso_quotidien_electricite_gaz_nationale = get_data_conso_quotidien_electricite_gaz_nationale(data=[],limit=100, offset=0)

        df = pd.DataFrame(data=conso_quotidien_electricite_gaz_nationale)
        df.to_csv("../data/conso_quotidien_electricite_gaz_nationale.csv",index=False)

    except Exception as e:
        logging.error(f'An error occured: {e}')




# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

