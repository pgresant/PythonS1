##### Commandes d'installation des librairies
"""
pip install py7zr, zipfile, tempfile
"""

##### Librairies
import datetime
import pandas as pd
import requests
import zipfile
import os
import io
import tempfile
import py7zr

##### Chemin de sauvegarde des données
def get_path(nomFichier):
    date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    return "./donnees/" + nomFichier + "_" + date + ".csv"

##### Fonctions d'importation et nettoyage des données
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
    # WARNING ECHEC
    for df in [nbIVG, nbIVG_anesth]:
        df["zone_geo"] = df["zone_geo"].replace("Total IVG réalisées en France", "France entière")
    # jointure sur les colonnes département et annee
    data_IVG = nbIVG
    for other_df in [nbIVG_anesth, nbIVG_tardiv, nbIVG_horsdept, nbIVG_minpro, nbIVG_age, nbIVG_typepro]:
        data_IVG = pd.merge(data_IVG, other_df, on=['zone_geo', 'annee'], how='outer')

    # retirer les lignes vides : NE FONCTIONNE PLUS APRES LES MODIF 
    data_IVG = data_IVG.dropna(subset=['zone_geo'])
    
    return data_IVG

def importer_finess(): 
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

def openZip(url, cheminCSV, temp_dir, encodage=None, sevenZip=False):
    """
    Téléchargement d'un fichier ZIP se trouvant à l'url,
    extraction du fichier CSV, contenu dans le zip à l'emplacement cheminCSV,
    avec un encodage précisé ou par défault None.
    Retourne un dataframe des données ducsv ouvert.
    """
    response = requests.get(url)
    response.raise_for_status()


    with io.BytesIO(response.content) as f:
        if sevenZip:
            zipFunction = py7zr.SevenZipFile(f, mode='r')
            zipFunction.extract(targets=[cheminCSV], path=temp_dir)
            extracted_path = os.path.join(temp_dir, cheminCSV)
            df = pd.read_csv(extracted_path, sep=";",encoding=encodage)
        else:
            zipFunction = zipfile.ZipFile(f, mode='r')
            zipFunction.extract(cheminCSV, path=temp_dir)
            extracted_path = os.path.join(temp_dir, cheminCSV)
            df = pd.read_csv(extracted_path, sep=";",encoding=encodage)

        return df

def importer_departement(): 
    """
    Extraction du csv contenant le nombre d'habitant par département en France en 2022. 
    Nettoyage rapide. 
    """
    url = "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip"
    csv = "donnees_departements.csv"

    with tempfile.TemporaryDirectory() as temp_dir: 
        dept = openZip(url, csv, temp_dir)

    # retirer les colonnes inutilisees du csv 
    dept = dept.drop(["NBARR", "NBCAN", "NBCOM", "PTOT"], axis=1)

    return dept

def importer_SAE():
    """
    Extraction du csv 
    """
    for annee in ["2023"]:
        url = "https://data.drees.solidarites-sante.gouv.fr/api/v2/catalog/datasets/708_bases-statistiques-sae/attachments/sae_" + annee + "_bases_statistiques_formats_sas_csv_7z"
        chemin = "SAE " + annee + " Bases statistiques - formats SAS-CSV/Bases statistiques/Bases CSV/"
        csvPERINAT = chemin + "PERINAT" + "_" + annee + "r.csv"
        csvID = chemin + "ID" + "_" + annee + "r.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            # dfFull = openZip(url, csvPERINAT, temp_dir, encodage="utf-8", sevenZip=True)
            dfFull = openZip(url, csvPERINAT, temp_dir, encodage="latin1", sevenZip=True)
            dfID = openZip(url, csvID, temp_dir, encodage="latin1", sevenZip=True)
        
        dfPERINAT = dfFull[["FI", "PRIS", "IVG", "IVGN_1", "IVGME", "IVG1214", "CONV", "IMG"]]
        dfPERINAT = dfPERINAT[dfPERINAT["PRIS"] == 1.0]
        dfID["FI"] = dfID["fi"]

        dfMerged = pd.merge(
            dfPERINAT,
            dfID[["FI", "rs", "stj", "stjr", "cat", "catr", "dep"]],
            on="FI",
            how="left" 
        )
        dfMerged["Annee"] = annee

    return dfMerged

def importer_pauv():
    """
    Extraction du tableau excel des taux de pauvreté par département en 2021. 
    """
    xls = pd.ExcelFile("https://www.insee.fr/fr/statistiques/fichier/7941411/RPM2024-F21.xlsx")
    pauv = pd.read_excel(xls, 'Figure 2')          
    return pauv
    
def main():
    """
    Sauvegarde et centralisation de toutes les importations de données
    """
    # df_drees = importer_drees()
    # df_drees.to_csv(get_path("drees"), index=False)

    # df_dep = importer_departement()
    # df_dep.to_csv(get_path("departements"), index=False)

    # df_finess = importer_finess()
    # df_finess.to_csv(get_path("finess"), index=False)

    df_SAE = importer_SAE()
    df_SAE.to_csv(get_path("SAE"), index=False)

# main()s