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
def get_path(nomFichier, date=None):
    """
    Retourne le chemin des sauvegardes de données
    """
    if date is None: 
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    
    return "./donnees/" + nomFichier + "_" + date + ".csv"

##### Fonctions d'importation et nettoyage des données
def importer_drees():
    """
    Importation des données de la DREES sur les IVGs, sous la forme de csv. 
    8 feuilles avec des données différentes portant sur le nombre d'IVGs par département, 
    leur type et caractéristiques, ainsi que ceux des personnes y ayant recours.
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
        df.rename(columns={'ANNEE': 'annee'}, inplace=True)

    for df in [nbIVG, nbIVG_anesth, nbIVG_tardiv]:
        df["zone_geo"] = df["zone_geo"].replace("Total IVG réalisées en France", "France entière")

    # jointure sur les colonnes département et annee
    data_IVG = nbIVG
    for other_df in [nbIVG_anesth, nbIVG_tardiv, nbIVG_horsdept, nbIVG_minpro, nbIVG_age, nbIVG_typepro]:
        data_IVG = pd.merge(data_IVG, other_df, on=['zone_geo', 'annee'], how='outer')

    # retirer les lignes vides
    data_IVG = data_IVG.dropna(subset=['zone_geo'])
    data_IVG['département'] = data_IVG['zone_geo']
    data_IVG = data_IVG.drop(columns=['Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'CAB_GO', 'CAB_MG',
       'CAB_SF', 'CAB_AUT', 'TELE_GO', 'TELE_MG', 'TELE_SF', 'TELE_AUT', 'zone_geo'])
    
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
    Retourne un dataframe des données du csv ouvert.
    """
    response = requests.get(url)
    response.raise_for_status()

    with io.BytesIO(response.content) as f:
        if sevenZip:
            zipFunction = py7zr.SevenZipFile(f, mode='r')
            zipFunction.extract(targets=[cheminCSV], path=temp_dir)
            extracted_path = os.path.join(temp_dir, cheminCSV)
        else:
            zipFunction = zipfile.ZipFile(f, mode='r')
            zipFunction.extract(cheminCSV, path=temp_dir)
            extracted_path = os.path.join(temp_dir, cheminCSV)
            
        df = pd.read_csv(extracted_path, sep=";",encoding=encodage)
        return df

def importer_departement(): 
    """
    Creation d'une dataframe du nombre de femmes de 15 à 49 ans
    selon le département avec des données INSEE 2025. 
    """
    url = "https://www.insee.fr/fr/statistiques/fichier/8331297/estim-pop-dep-sexe-aq-1975-2025.xlsx"
    df = pd.read_excel(url, '2025', header=4, skipfooter=4)

    df.rename(columns={'Unnamed: 0': 'code_dep', 'Unnamed: 1': 'département'}, inplace=True)
    df['femmes'] = df['15 à 19 ans'] + df['20 à 24 ans'] + df['25 à 29 ans'] + df['30 à 34 ans'] + df['35 à 39 ans'] + df['40 à 44 ans'] + df['45 à 49 ans']
    df = df[['code_dep', 'département', 'femmes']]

    return df

def importer_SAE_2024():
    """
    Téléchargement des données SAE de 2024 : filtres et jointure entre la base PERINAT
    qui regroupe les données sur les IVGs et ID avec des données administratifs.
    Fonction adaptée à la version post-2014 de la base SAE.
    """
    annee = '2024'
    url = "https://data.drees.solidarites-sante.gouv.fr/api/v2/catalog/datasets/708_bases-statistiques-sae/attachments/sae_" + annee + "_bases_statistiques_formats_sas_csv_7z"
    chemin = "SAE " + annee + " Bases statistiques - formats SAS-CSV/Bases statistiques/Bases CSV/"
    csvPERINAT = chemin + "PERINAT" + "_" + annee + "r.csv"
    csvID = chemin + "ID" + "_" + annee + "r.csv"

    with tempfile.TemporaryDirectory() as temp_dir:
        dfFull = openZip(url, csvPERINAT, temp_dir, encodage="latin1", sevenZip=True)
        dfID = openZip(url, csvID, temp_dir, encodage="latin1", sevenZip=True)
    
    dfPERINAT = dfFull[["FI", "PRIS", "IVG", "IVGN_1", "IVGME", "IVG1214", "IVG1516", "CONV", "IMG"]]
    dfPERINAT = dfPERINAT[dfPERINAT["PRIS"] == 1.0]
    dfID.rename(columns={'fi': 'FI', 'dep': 'DEP'}, inplace=True)

    dfMerged = pd.merge(
        dfPERINAT,
        dfID[["FI", "rs", "stj", "stjr", "cat", "catr", "DEP"]],
        on="FI",
        how="left" 
    )

    dfMerged.rename(columns={'DEP': 'code_dep'}, inplace=True)

    return dfMerged

def importer_SAE_2011():
    """
    Téléchargement des données SAE 2011 : filtre sur la seule table publique de la SAE.
    Fonction adaptée à la version pré-2014 de la base SAE.
    """
    sae = pd.read_excel("https://www.data.gouv.fr/storage/f/2014-01-10T17-34-07/SAE_2011.xls")
    sae = sae[['FI', 'RS', 'PRIS', 'nb_ivg', 'nb_ivg_medic', 'delai_moy_pec_ivg', 'CATEGORIE', 'DEP']]

    sae.rename(columns={'DEP': 'code_dep'}, inplace=True)

    return sae

def importer_pauv():
    """
    Extraction du tableau excel des taux de pauvreté par département 
    en 2021 d'après l'INSEE. 
    """
    url = "https://www.insee.fr/fr/statistiques/fichier/7941411/RPM2024-F21.xlsx"
    df_pauv = pd.read_excel(url, 'Figure 2', header=2, skipfooter=4)
    df_pauv.columns = ['code_dep', 'Département', 'taux_pauvrete'] 
    df_pauv = df_pauv[['code_dep', 'taux_pauvrete']]

    return df_pauv

#### Fonctions d'importation globale des données
def importer_tout():
    df_SAE = importer_SAE_2024()
    df_SAE_2011 = importer_SAE_2011()
    df_dep = importer_departement()
    df_finess = importer_finess()
    df_drees = importer_drees()
    df_pauv = importer_pauv()

    return df_SAE, df_SAE_2011, df_dep, df_finess, df_drees, df_pauv

def sauvegarde_locale():
    df_SAE, df_SAE_2011, df_dep, df_finess, df_drees, df_pauv = importer_tout()
    df_SAE.to_csv(get_path("SAE"), index=False)
    df_SAE_2011.to_csv(get_path("SAE_2011"), index=False)
    df_dep.to_csv(get_path("dep"), index=False)
    df_finess.to_csv(get_path("finess"), index=False)
    df_drees.to_csv(get_path("drees"), index=False)
    df_pauv.to_csv(get_path("pauv"), index=False)

    return 1