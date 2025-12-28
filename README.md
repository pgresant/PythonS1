# Analyse territorialisée des interventions volontaires de grosssesse : les disparités d'accès en France

*Par Aika Mizuno Greer et Polixène Grésant, 2025.*

# Table des matières
1. [Sujet et question de recherche](#suj)
2. [Données utilisées](#data)
3. [Méthodes d'analyse](#mod)
4. [Présentation du dépôt](#pres)


## 1. Sujet et question de recherche <a name="suj">

En 2024, la liberté de recourir à une IVG est inscrite dans la Constitution française pour la première fois. Cette liberté est désormais protégée juridiquement. Néanmois, les difficultés pratiques auxquelles sont confrontées les personnes souhaitant avorter révèlent des inégalités devant un théorique droit à avorter, indépendemment de la liberté érigée récemment. Des inégalités d'accès persistent sur le territoire français. 

Nous cherchons ainsi à établir une classification des départements en fonction de comment se déroulent les IVGs et à leur recours. L'objectif est d'établir une typologie, afin de décrire plus précisèment les différences d'accès à l'IVG selon les départements en France.


**Source** : L'accès à l'avortement dans l'Union européenne, Science Po, 2024 : https://www.sciencespo.fr/gender-studies/fr/actualites/acces-a-l-ivg-des-inegalites-persistantes-en-europe/


## 2. Données utilisées  <a name="data">

Afin d'avoir des données concernant l'accessibilité de l'IVG, nous utilisons deux sources de données : Doctolib et les SAE.
Les données SAE nous donnent le nombre de points d'accès aux IVG par département, dependant leurs données sur les délais d'attente pour un RDV sont incomplètes ou indisponibles. Ainsi, les données scrappées depuis Doctolib comblent ce manque : elles permettent d'avoir accès à l'offre des rendez-vous pour avorter sur un territoire à une période donnée, très récente. Doctolib exerce en effet une position dominante sur le marché des rendez-vous médicaux. 

Pour obtenir des données sur les caractéristiques des IVGs en France, nous avons utilisés les données produites par la DREES. Finalement, l'INSEE nous fourni les caractéristiques socio-économiques des départements.

Toutes les données, sauf le scraping Doctolib, sont disponibles en open source. Tout de même, une version locale des données est sauvegardée dans le dossier `./donnees/` pour éviter le temps de téléchargement qui peut prendre quelques minutes.

## 3. Méthodes d'analyse <a name="mod">

Nous mobilisons principalement des statistiques descriptives, une classification ascendante hiérarchique (CAH) ainsi que des régressions linéaires avec la méthode des moindres carrés ordinaires et des régressions logistiques.

## 4. Présentation du dépôt <a name="pres">

Notre projet s'appuie sur quatre scripts qui se trouvent dans le dossier `./scripts/` : 
- `importation_donnees_tabulaires.py` pour les fonctions concernant le téléchargement et nettoyage des sources de données en open data
- `scraping_doctolib.py` pour le web-scraping de Doctolib
- `compilation_donnees.py` pour la création de variables ou d'agrégations à partir des sources précédentes, et la jointure finale des données
- `visualisation.py` pour la création de data visualisation et les fonctions de statistiques descriptives, ainsi que de modélisation. 

Le rendu final est présenté dans le notebook `main.ipynb` que nous recommandons [d'ouvrir sur nbviewer](https://nbviewer.org/github/pgresant/PythonS1/blob/main/main.ipynb) pour éviter des problèmes d'affichage sur Github.


