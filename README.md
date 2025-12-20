# Analyse territorialisé des interventions volontaires de grosssesse : les disparités d'accès en France

*Par Aika Mizuno Greer et Polixène Grésant, 2025.*

# Table des matières
1. [Sujet et question de recherche](#suj)
2. [Données utilisées](#data)
3. [Méthodes d'analyse](#mod)
4. [Présentation du dépôt](#pres)


## 1. Sujet et question de recherche <a name="suj">

En 2024, la liberté de recourir à une IVG est inscrite dans la Constitution française pour la première fois. Cette liberté est désormais protégée juridiquement. Néanmois, les difficultés pratiques auxquelles sont confrontées les personnes souhaitant avorter révèlent des inégalités devant un théorique droit à avorter, indépendemment de la liberté érigée récemment. Des inégalités d'accès persistent sur le territoire français. 

Nous cherchons ainsi à établir une classification des départements en fonction de comment se déroule les IVGs et à leur recours. L'objectif est d'établir une typologie, afin de décrire plus précisèment les différences d'accès à l'IVG selon les départements en France.
Dans cette logique, afin d'expliquer les différences de recours au sein d'un département, nous souhaitons tester si le nombre d'IVGs est explicable notamment selon les délais d'attente. 

**Source** : L'accès à l'avortement dans l'Union européenne, Science Po, 2024 : https://www.sciencespo.fr/gender-studies/fr/actualites/acces-a-l-ivg-des-inegalites-persistantes-en-europe/


## 2. Données utilisées  <a name="data">

Afin d'avoir des données concernant l'accessibilité de l'IVG, nous utilisons deux sources de données : Doctolib et les SAE. Les données scrappées sur Doctolib permettent d'avoir accès à l'offre des rendez-vous pour avorter sur un territoire à une période donnée, très récente. Doctolib exerce en effet une position dominante sur ce marché des rendez-vous médicaux. D'autres part, les données SAE produites par la DREES permettent de connaitre les délais d'attente par centre médicale depuis 2016. 

Pour obtenir des données sur les caractéristiques des IVGs en France, nous avons utilisés les données produites par la DREES. 

Nous avons privilégier d'extraire les données via des API publics dès qu'il était possible de le faire. 


## 3. Méthodes d'analyse <a name="mod">

Nous mobilisons principalement des statistiques descriptives, une classification ascendante hiérarchique (CAH) ainsi que des régressions linéaires avec la méthodes des moindres carrés ordinaires.  

## 4. Présentation du dépôt <a name="pres">

Le rendu finale est présenté dans le notebook `main.ipynb`. Quatre scripts se trouvent dans le dossier `./scripts/` : 
- `importation_donnees_tabulaires.py` qui