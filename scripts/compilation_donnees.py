""""
commande d'installation des librairies : 
%pip install py7zr
""""

import pandas as pd
import requests
import zipfile
import os
import io
import tempfile
import py7zr

def importer_drees():
    """
    Importation des données de la DREES sur les IVGs, sous la forme de csv. 
    8 feuilles avec des données différentes portant sur le nombre d'IVGs par département, leur type et caractéristiques, ainsi que ceux des personnes y ayant recours.
    Assemblage des 8 fichiers dans un unique fichier. 
    """
    dress_feuil1 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil1_csv/"
    nbIVG = pd.read_csv(dress_feuil1, sep=";", encoding="latin")

    dress_feuil2 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil2_csv/"
    nbIVG_anesth = pd.read_csv(dress_feuil2, sep=";", encoding="latin")

    dress_feuil3 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil3_csv/"
    nbIVG_tardiv = pd.read_csv(dress_feuil3, sep=";", encoding="latin")

    dress_feuil5 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil5_csv/"
    nbIVG_horsdept = pd.read_csv(dress_feuil5, sep=";", encoding="latin")

    dress_feuil7 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil7_csv/"
    nbIVG_minpro = pd.read_csv(dress_feuil7, sep=";", encoding="latin")

    dress_feuil8 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil8_csv/"
    nbIVG_age = pd.read_csv(dress_feuil8, sep=";", encoding="latin")

    dress_feuil9 = "https://data.drees.solidarites-sante.gouv.fr/api/datasets/1.0/3647_ivg/attachments/donnees_feuil9_csv/"
    nbIVG_typepro = pd.read_csv(dress_feuil9, sep=";", encoding="latin")
    
    # homogénéisation des noms de colonnes qui serviront à la jointure  
    for df in [nbIVG, nbIVG_anesth, nbIVG_tardiv, nbIVG_horsdept, nbIVG_minpro, nbIVG_age, nbIVG_typepro]:
        df.rename(columns={'ZONE_GEO': 'zone_geo'}, inplace=True)
    for df in [nbIVG, nbIVG_anesth, nbIVG_tardiv, nbIVG_horsdept, nbIVG_minpro, nbIVG_age, nbIVG_typepro]:
        df.rename(columns={'ANNEE': 'annee'}, inplace=True)

    # jointure sur les colonnes département et annee
    data_IVG = nbIVG
    for other_df in [nbIVG_anesth, nbIVG_tardiv, nbIVG_horsdept, nbIVG_minpro, nbIVG_age, nbIVG_typepro]:
        data_IVG = pd.merge(data_IVG, other_df, on=['zone_geo', 'annee'], how='outer')

    return data_IVG


url = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"
csv = "donnees_departements.csv"

def openFile(z, csv):
 """
 Ouverture d'un fichier ZIP et extraction du fichier CSV depuis l’archive ZIP, grâce à la construction d'un chemin.
 """
    with tempfile.TemporaryDirectory() as temp_dir: 
        z.extract(csv, path=temp_dir)
        extracted_path = os.path.join(temp_dir, csv)
        df = pd.read_csv(extracted_path,sep=";")
    return df

def importer_departement(): 
    """
    Extraction du csv contenant le nombre d'habitant par département en France en 2022. 
    Nettoyage rapide. 
    """
    url = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"
    csv = "donnees_departements.csv"
    response = requests.get(url)
    response.raise_for_status()
    with io.BytesIO(response.content) as f:
     with zipfile.ZipFile(f, mode='r') as z:
        dept = openFile(z, csv)

    # retirer les colonnes inutilisees du csv 
    dept = dept.drop(["NBARR", "NBCAN", "NBCOM", "PTOT"], axis=1)

    return dept

def importer_finess() : 
    """
    Extraction des donnees du fichier national des établissements sanitaires et sociaux
    Nettoyage pour conserver uniquement les lignes et colonnes d'intérêt. 
    """
    fin = "https://static.data.gouv.fr/resources/finess-extraction-du-fichier-des-etablissements/20251106-144812/etalab-cs1100507-stock-20251106-0338.csv"
    finess = pd.read_csv(fin, sep=";", encoding="latin", header=None, skiprows=1)
    # retirer les lignes de géolocalisation
    finess = finess[finess[0] == "structureet"]
    # retirer les informations non nécessaires au projet 
    finess = finess[[1, 2, 3, 4, 14, 19, 21]]
    return finess


def getUrl(annee):
    """
    Retourner l'url correspondant à l'année dont des données DREES sont extraites. 
    """
    return f"https://data.drees.solidarites-sante.gouv.fr/api/v2/catalog/datasets/708_bases-statistiques-sae/attachments/sae_{annee}_bases_statistiques_formats_sas_csv_7z"

def csvName(file, annee):
    """
    """
    return f"SAE {annee} Bases statistiques - formats SAS-CSV/Bases statistiques/Bases CSV/{file}_{annee}r.csv"


output = "Data/SAE/"
url = getUrl(2019)
csvPERINAT = csvName("PERINAT", 2019)
csvID = csvName("ID", 2019)

def openFile(csv, encodage):
    response = requests.get(url)
    response.raise_for_status()

    with io.BytesIO(response.content) as f:
        with py7zr.SevenZipFile(f, mode='r') as z:
            if csv in z.getnames():
                with tempfile.TemporaryDirectory() as temp_dir:
                    z.extract(targets=[csv], path=temp_dir)
                    extracted_path = os.path.join(temp_dir, csv)
                    df = pd.read_csv(extracted_path,sep=";",encoding=encodage)

                    return df
            else:
                print(f"{csv} not found in the archive.")
                return None

dfFull = openFile(csvPERINAT, "utf-8")
dfPERINAT = dfFull[["FI", "PRIS", "IVG", "IVGN_1", "IVGME", "IVG1214", "CONV", "IMG"]]
dfPERINAT = dfPERINAT[dfPERINAT["PRIS"] == 1.0]

dfID = openFile(csvID, "latin1")
dfID["FI"] = dfID["fi"]

dfMerged = pd.merge(
    dfPERINAT,
    dfID[["FI", "rs", "stj", "stjr", "cat", "catr", "dep"]],
    on="FI",
    how="left" 
)
dfMerged["Annee"] = "2019"

# supprimer le dossier crée dans téléchargement

def main():
    importer_dress()
    importer_departement()
    importer_finess()


main()