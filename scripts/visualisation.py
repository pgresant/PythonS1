#### Librairies
import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
import matplotlib
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
import plotly.express as px
import folium
import mapclassify

#### Fonctions de visualisation simple
def get_gdf(df):
    """
    Ajoute les données spatiales au dataframe df pour
    pourvoir afficher les départements sur une carte
    """
    # Geojson de l'INSEE pour les département
    urlGeojson = "https://www.data.gouv.fr/fr/datasets/r/90b9341a-e1f7-4d75-a73c-bbc010c7feeb" 
    geo = gpd.read_file(urlGeojson)

    # Standardisation de la typographie des codes départements
    geo['code_dep'] = geo['code'].astype(str).str.zfill(2)

    # Ajout de la colonne geometry et conversion en gdf
    gdf = gpd.GeoDataFrame(pd.merge(df, geo, on='code_dep', how='left'))
    return gdf

def correlation(df, suffix_title=""):
    """
    Affichage d'une matrice de corrélation de la dataframe
    df, avec un suffixe possible pour le titre du graphique
    """
    corr = df.corr()
    mask = np.triu(np.ones_like(corr, dtype=bool))
    plt.figure(figsize=(16, 12))
    sns.heatmap(
        corr,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap='coolwarm',
        center=0,
        square=True,
        annot_kws={"size": 8},
        cbar_kws={"shrink": 0.8}
    )
    plt.title('Matrice de Corrélation' + suffix_title, fontsize=16, pad=20)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    plt.show()

#### Fonctions de régression
def OLS(main, x_col, y_col):
    """
    Affiche les caractérisques et le graphe d'une 
    régression linéaire simple de y_col sur x_col 
    dans les données main
    """
    X = main[[x_col]]
    X = sm.add_constant(X)
    y = main[y_col]

    # Méthode OLS
    model = sm.OLS(y, X).fit()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5), gridspec_kw={'width_ratios': [6, 5]})
    summary_text = str(model.summary())
    ax1.text(0, 0.5, summary_text, va='center', ha='left', fontsize=10, family='monospace')
    ax1.axis('off') 
    sns.scatterplot(x=x_col, y=y_col, data=main, ax=ax2,   color='skyblue')
    sns.regplot(x=x_col, y=y_col, data=main, scatter=False, ax=ax2, color = 'sandybrown')
    ax2.set_title(f'Regression Simple de {y_col} sur {x_col}')
    plt.tight_layout()
    plt.show()

def regression_logistique(main, num_cluster):
    """
    Affiche les caractéristiques d'une régression logisitique
    de la classe num_cluster sur le taux de pauvreté.
    Main est le dataframe des données. 
    """
    # Creation d'une variable binaire d'appartenance à num_cluster
    main['bin'] = main['cluster'].apply(lambda x: 1 if x == num_cluster else 0)

    X = main[["taux_pauvrete"]]
    X = sm.add_constant(X)
    y = main["bin"]

    # Régression logistique
    model = sm.Logit(y, X).fit()

    # Calcul et affichage des effets marginaux
    marginal_effect = model.get_margeff(at='overall')

    return marginal_effect


##### Dendogramme CAH
def dendrogramme_CAH(df, Z):
    """
    Affiche un dendogramme associé au CAH Z sur df.
    """
    plt.figure(figsize=(10, 6))
    dendrogram(
        Z,
        labels=df["code_dep"].values,
        leaf_rotation=45
    )
    plt.title("Dendrogramme - CAH")
    plt.xlabel("Département")
    plt.ylabel("Distance")
    plt.show()


##### Fonctions de visualisation des caractéristiques du clustering
def violon(main, x_col, y_col, couleurs, titre=""):
    """
    Affichage de graphiques de distribution en violon
    de la variable y_col selon la variable x_col
    Les distributions sont côte-à-côte
    """
    plt.figure(figsize=(10, 6))

    sns.violinplot(
        data=main,
        x=x_col,
        y=y_col,
        inner=None,
        palette=couleurs,
        hue='cluster'
    )
    sns.boxplot(
        data=main,
        x=x_col,
        y=y_col,
        width=0.15,
        showcaps=True,
        boxprops={"facecolor": "white", "zorder": 2},
        showfliers=False,
        whiskerprops={"linewidth": 1.5}
    )
    plt.title(titre)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.tight_layout()
    plt.show()

def camembert_cluster(main):
    """
    Affichage de graphiques circulaires pour la distribution
    des ages selon la classe dans les données main
    """
    age_cols = ["age_inf_18", "age_18_19", "age_20_24",
        "age_25_29", "age_30_34", "age_35_39", "age_sup_40"]

    # Moyenne des parts par cluster
    df_cluster = (
        main.groupby("cluster")[age_cols]
        .mean()
    )

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    axes = axes.flatten()
    colors=matplotlib.color_sequences['Set2']

    for i, cluster in enumerate(df_cluster.index):
        values = df_cluster.loc[cluster]
        axes[i].pie(
            values,
            labels=age_cols,
            autopct='%1.1f%%',
            startangle=90,
            colors=colors
        )
        axes[i].set_title(f"Cluster {cluster}")

    plt.tight_layout()
    plt.show()

def boxplot(df, colonnes, couleurs):
    """
    Affichage dynamique de boxplot, avec dropdown
    pour choisir la colonne représentée parmis
    la liste des colonnes en argument
    """
    clusters = [1, 2, 3, 4]

    fig = px.box(df, y=colonnes[0], color="cluster", color_discrete_sequence=couleurs)

    buttons = []
    for num_var in colonnes:
        buttons.append({
            "method": "update",
            "label": num_var,
            "args": [
                {"y": [df[df["cluster"]==cluster][num_var] for cluster in clusters]},
                {"title": f"Boxplot de {num_var} selon la classe", "yaxis.title": num_var} 
            ]
        })

    fig.update_layout(
        updatemenus=[{
            "buttons": buttons,
            "direction": "down",
            "showactive": True,
            "y":1,
            "x":1
        }]
    )
    fig.show()