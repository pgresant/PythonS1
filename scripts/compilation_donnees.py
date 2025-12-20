##### Librairies
import pandas as pd

#### Importation d'une sauvegarde locale de données 
def get_path(nomFichier, date=None):
    """
    Retourne le chemin des sauvegardes de données
    """
    if date is None: 
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    
    return "./donnees/" + nomFichier + "_" + date + ".csv"

def importer_locale(date, dateDoctolib):
    """
    Importe une sauvegarde locale (date en parametre) des bases de données
    Retourne les dataframes associes
    """
    df_SAE = pd.read_csv(get_path("SAE", date))
    df_dep = pd.read_csv(get_path("dep", date))
    df_finess = pd.read_csv(get_path("finess", date))
    df_drees = pd.read_csv(get_path("drees", date))
    df_pauv = pd.read_csv(get_path("pauv", date))
    df_doctolib = pd.read_csv(get_path("doctolib", dateDoctolib))

    return df_SAE, df_dep, df_finess, df_drees, df_pauv, df_doctolib

#### Fonctions d'agrégation (spatiale ou par groupe)
def agregation_finess(df_finess):
    """
    Agrégation spatiale par département pour compter le nombre de 
    centre de santé sexuelle et de ceGIDD par département.
    Retourne un dataframe.
    """
    df_nbcentre = pd.crosstab(df_finess["14"], df_finess["19"])
    df_nbcentre.index.name = "département"
    df_nbcentre.rename(columns={
        'Centre de santÃ© sexuelle': 'centre_de_sante_sexuelle', 
        "Centre gratuit d'information de dÃ©pistage et de diagnostic" : 'ceGIDD'
    }, inplace=True)
    df_nbcentre = df_nbcentre[["centre_de_sante_sexuelle", "ceGIDD"]]
    df_nbcentre["centres_total"] = df_nbcentre["centre_de_sante_sexuelle"] + df_nbcentre["ceGIDD"]

    return df_nbcentre

def agregation_doctolib(df_doctolib):
    """
    Compte le nombre d'établissement offrant des RDV IVGs
    sans prendre en compte le type de l'IVG.
    """
    df_doctolib = df_doctolib.drop(columns=['departement'])
    df_doctolib = df_doctolib.groupby(['code_dep'], as_index=False).agg({
        'trimestre': 'sum',
        'mois': 'sum',
        'deuxSemaines': 'sum'
    })
    df_doctolib.rename(columns={'trimestre': 'doctolib_trimestre', 'mois': 'doctolib_mois', 'deuxSemaines': 'doctolib_2sem'}, inplace=True)

    return df_doctolib

def agregation_SAE(df_SAE):
    """
    Agrégation spatiale par département de la base SAE.
    Retourne un DataFrame avec des comptages par département 
    - du nombre d'établissement ayant pris en charge au moins un IVG
    - du nombre d'établissement ayant pris en charge au moins un IVG 
      mais aucun IVG tardif
    - du nombre d'établissement conventinnée pour faire des IVGs
    """
    prise_en_charge = (
        df_SAE
        .groupby('code_dep')[['PRIS', 'CONV']]
        .sum()
        .reset_index()
    )

    ivg_sans_tard = (
        df_SAE
        .loc[
            (df_SAE['IVG1214'].fillna(0) == 0) &
            (df_SAE['IVG1516'].fillna(0) == 0)
        ]
        .groupby('code_dep')
        .size()
        .reset_index(name='hopitaux_sans_ivg_tard')
    )
    ivg_sans_tard['hopitaux_sans_ivg_tard'] = ivg_sans_tard['hopitaux_sans_ivg_tard'].fillna(0)

    df_final = pd.merge(prise_en_charge,  ivg_sans_tard, on='code_dep')

    return df_final


def jointure(df_SAE, df_dep, df_finess, df_drees, df_pauv, df_doctolib):
    """
    Fonction de jointure des sources de données tabulaires.
    """
    # Agrégations sur les données brutes
    df_nbcentre = agregation_finess(df_finess)
    df_doctolib_agreg = agregation_doctolib(df_doctolib)
    df_SAE_agreg = agregation_SAE(df_SAE)
    
    # Typographie alternative pour permettre la jointure avec les données finess
    df_dep['DEP'] = (
        df_dep['département']
            .str.replace('-', ' ', regex=False)
            .str.upper()
    )

    # Enlever les lignes concernant les DROMs (données manquantes)
    droms = ['971', '972', '973', '974', '976'] 
    df_dep_metro = df_dep[~df_dep['code_dep'].isin(droms)]
    df_dep_metro = df_dep_metro.dropna(subset=["DEP"])

    # Jointure des sources standardisées
    df = pd.merge(df, df_nbcentre, left_on=['DEP'], right_on=['département'], how='left')
    df.drop(columns=['DEP'], inplace=True)

    df_drees_2024 = df_drees[df_drees['annee'] == 2024.0]
    df_drees_2024 = df_drees_2024.drop(columns=['annee'])
    df = pd.merge(df, df_drees_2024, on=['département'], how='left')
    df = pd.merge(df, df_pauv, on=['code_dep'], how='left')    
    df = pd.merge(df, df_doctolib_agreg, on=['code_dep'], how='left')
    df = pd.merge(df, df_SAE_agreg, on=['code_dep'], how='left')

    df.columns = map(str.lower, df.columns) # Mettre les noms de colonnes en minuscules

    return df

def par100k(df, colonne):
    """
    Normalise une colonne de df pour avoir le taux par 100 000 femmes en
    âge de procréer
    """
    df[colonne] = pd.to_numeric(df[colonne], errors='coerce')
    
    return (df[colonne]/df["femmes"])*100000

def normalisation(df, colonnes_intactes=[]):
    """
    Normalise toute les colonnes numériques de notre df sauf ceux précisées dans
    le paramètre colonnes_intactes.
    """
    if len(colonnes_intactes) == 0:
        colonnes_intactes = ['code_dep', 'département', 'femmes', 'taux_pauvrete', 'taux_rec', 
                             'part_ivg_tard', 'ratio_ivg_nais', 'part_inf_18', 'part_18_19', 
                             'part_20_24', 'part_25_29', 'part_30_34', 'part_35_39', 'part_sup_40']
    else:
        colonnes_intactes = colonnes_intactes + ['code_dep', 'département', 'departement']
    
    colonnes = df.columns
    colonnes_norm = [c for c in colonnes if c not in colonnes_intactes]

    norm = df.copy()

    for c in colonnes_norm:
        norm[c] = par100k(norm, c)

    return norm