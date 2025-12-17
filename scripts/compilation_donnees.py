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
    df_SAE_2011 = pd.read_csv(get_path("SAE_2011", date))
    df_dep = pd.read_csv(get_path("dep", date))
    df_finess = pd.read_csv(get_path("finess", date))
    df_drees = pd.read_csv(get_path("drees", date))
    df_pauv = pd.read_csv(get_path("pauv", date))
    df_doctolib = pd.read_csv(get_path("doctolib", dateDoctolib))

    return df_SAE, df_SAE_2011, df_dep, df_finess, df_drees, df_pauv, df_doctolib

#### Aggregation et normalisation des données
def aggreg_doctolib(df_doctolib):
    df_doctolib = df_doctolib.drop(columns=['departement'])

    df_doctolib = df_doctolib.groupby(['code_dep'], as_index=False).agg({
        'trimestre': 'sum',
        'mois': 'sum',
        'deuxSemaines': 'sum'
    })

    df_doctolib.rename(columns={'trimestre': 'doctolib_trimestre', 'mois': 'doctolib_mois', 'deuxSemaines': 'doctolib_2sem'}, inplace=True)

    return df_doctolib


def jointure(df_SAE, df_SAE_2011, df_dep, df_finess, df_drees, df_pauv, df_doctolib):
    """
    Fonction de jointure des sources de données tabulaires.
    """
    df_nbcentre = pd.crosstab(df_finess["14"], df_finess["19"])
    df_nbcentre.index.name = "département"
    df_nbcentre.rename(columns={
        'Centre de santÃ© sexuelle': 'centre_de_sante_sexuelle', 
        "Centre gratuit d'information de dÃ©pistage et de diagnostic" : 'ceGIDD'
    }, inplace=True)
    df_nbcentre = df_nbcentre[["centre_de_sante_sexuelle", "ceGIDD"]]
    df_nbcentre["centres_total"] = df_nbcentre["centre_de_sante_sexuelle"] + df_nbcentre["ceGIDD"]
    
    # nb centre qui prend en charge les IVG par département 
    df_prise_en_charge_dep = (
        df_SAE
        .groupby('code_dep')[['PRIS', 'CONV']]
        .sum()
        .reset_index()
    )

    df_ivg_sans_tard = (
        df_SAE
        .loc[
            (df_SAE['IVG1214'].fillna(0) == 0) &
            (df_SAE['IVG1516'].fillna(0) == 0)
        ]
        .groupby('code_dep')
        .size()
        .reset_index(name='hopitaux_sans_ivg_tard')
    )
    df_ivg_sans_tard['hopitaux_sans_ivg_tard'] = df_ivg_sans_tard['hopitaux_sans_ivg_tard'].fillna(0)

    # Typographie alternative pour les noms de départements pour la jointure avec les données finess
    df_dep['DEP'] = (
        df_dep['département']
            .str.replace('-', ' ', regex=False)
            .str.upper()
    )
    # Enlever les lignes concernant les DROMs (données manquantes)
    droms = ['971', '972', '973', '974', '976'] 
    df_dep_metro = df_dep[~df_dep['code_dep'].isin(droms)]
    df_dep_metro = df_dep_metro.dropna(subset=["DEP"])
    df = pd.merge(df_dep_metro, df_prise_en_charge_dep, on=['code_dep'], how='left')
    df = pd.merge(df, df_nbcentre, left_on=['DEP'], right_on=['département'], how='left')
    df.drop(columns=['DEP'], inplace=True)
    df = pd.merge(df, df_ivg_sans_tard, on=['code_dep'], how='left')

    df_drees_2024 = df_drees[df_drees['annee'] == 2024.0]
    df_drees_2024 = df_drees_2024.drop(columns=['annee', 'IVG_GO', 'IVG_MG', 'IVG_SF', 'IVG_AUT', 'part_anesth'])
    df = pd.merge(df, df_drees_2024, on=['département'], how='left')
    df = pd.merge(df, df_pauv, on=['code_dep'], how='left')

    df_doctolib_aggreg = aggreg_doctolib(df_doctolib)
    df = pd.merge(df, df_doctolib_aggreg, on=['code_dep'], how='left')

    df.columns = map(str.lower, df.columns) # Mettre les noms de colonnes en minuscules

    df.rename(columns={'age_18&19': 'age_18_19', 'age_40&plus': 'age_sup_40'}, inplace=True)
    df['part_inf_18'] = df['age_inf_18']/df['tot_ivg']
    df['part_18_19'] = df['age_18_19']/df['tot_ivg']
    df['part_20_24'] = df['age_20_24']/df['tot_ivg']
    df['part_25_29'] = df['age_25_29']/df['tot_ivg']
    df['part_30_34'] = df['age_30_34']/df['tot_ivg']
    df['part_35_39'] = df['age_35_39']/df['tot_ivg']
    df['part_sup_40'] = df['age_sup_40']/df['tot_ivg']
    df['taux_rec'] = df['taux_rec'].str.replace(',', '.').astype(float)
    df['part_ivg_tard'] = df['part_ivg_tard'].str.replace(',', '.').astype(float)
    df['ivg_hors_zone'] = df['ivg_hors_zone'].str.replace(',', '.').astype(float)

    return df

def par100k(df, colonne):
    df[colonne] = pd.to_numeric(df[colonne], errors='coerce')
    
    return (df[colonne]/df["femmes"])*100000

def standardisation(df, colonnes_intactes=[]):
    if len(colonnes_intactes) == 0:
        colonnes_intactes = ['code_dep', 'département', 'femmes', 'taux_rec', 'part_ivg_tard', 'ratio_ivg_nais', 'part_age_inf_18', 'part_age_inf_18', 'part_age_18&19', 'part_age_20_24', 'part_age_25_29', 'part_age_30_34', 'part_age_35_39', 'part_age_40&plus']
    else:
        colonnes_intactes = colonnes_intactes + ['code_dep', 'département', 'departement']
    colonnes = df.columns
    
    # list comprehension
    colonnes_norm = [c for c in colonnes if c not in colonnes_intactes]

    norm = df.copy()

    for c in colonnes_norm:
        norm[c] = par100k(norm, c)

    return norm