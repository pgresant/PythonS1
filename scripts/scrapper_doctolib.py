##### Commandes d'installation des librairies
"""
pip install pytest-playwright, playwright_stealth, asyncio
sudo playwright install-deps
playwright install
"""

##### Librairies
from playwright.async_api import async_playwright, expect
from playwright_stealth import Stealth
import pandas as pd
import datetime, random, time
import asyncio

##### Chemin de sauvegarde des données
date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
PATH = "./donnees/doctolib_" + date + ".csv"

##### Options pour Playwright (web-scrapping dynamique par navigateur headless)
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.10 Safari/605.1.1	43.03",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.3	21.05",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.3	17.34",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.3	3.72",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Trailer/93.3.8652.5	2.48",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.	2.48",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.	2.48",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 OPR/117.0.0.	2.48",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.	1.24",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.	1.24"
]
semaphore = asyncio.Semaphore(5) # Nombre de pages ouvertes en concurrence (parrallélisme avec asyncio)
custom_languages = ("fr-FR", "fr") # Langue par défaut du navigateur
stealth = Stealth(navigator_languages_override=custom_languages)

##### Variables : URL Parameters & dataframe columns
columns = ["departement", "specialite", "trimestre", "mois", "deuxSemaines"]
baseUrl = "https://www.doctolib.fr/search"
argsSecteurs = "&regulationSector%5B%5D=CONTRACTED_1&regulationSector%5B%5D=CONTRACTED_1_WITH_EXTRA&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED_2&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED&regulationSector%5B%5D=CONTRACTED_WITH_EXTRA&regulationSector%5B%5D=ORGANIZATION_CONTRACTED"
listeSpecialites = ["ivg-chirurgicale", "ivg-medicale-et-chirurgicale", "ivg-medicamenteuse"]
listeDelais = ["90", "30", "14"]
listeDepartements = [
    "ain", "aisne", "allier", "alpes-de-haute-provence", "hautes-alpes",
    "alpes-maritimes", "ardeche", "ardennes", "ariege", "aube",
    "aude", "aveyron", "bouches-du-rhone", "calvados", "cantal",
    "charente", "charente-maritime", "cher", "correze", "corse-du-sud",
    "haute-corse", "cote-d-or", "cotes-d-armor", "creuse", "dordogne",
    "doubs", "drome", "eure", "eure-et-loir", "finistere",
    "gard", "haute-garonne", "gers", "gironde", "herault",
    "ille-et-vilaine", "indre", "indre-et-loire", "isere",
    "jura", "landes", "loir-et-cher", "loire", "haute-loire",
    "loire-atlantique", "loiret", "lot", "lot-et-garonne",
    "lozere", "maine-et-loire", "manche", "marne", "haute-marne",
    "mayenne", "meurthe-et-moselle", "meuse", "morbihan",
    "moselle", "nievre", "nord", "oise", "orne",
    "pas-de-calais", "puy-de-dome", "pyrenees-atlantiques",
    "hautes-pyrenees", "pyrenees-orientales", "bas-rhin",
    "haut-rhin", "rhone", "haute-saone", "saone-et-loire",
    "sarthe", "savoie", "haute-savoie", "paris", "seine-maritime",
    "seine-et-marne", "yvelines", "deux-sevres", "somme",
    "tarn", "tarn-et-garonne", "var", "vaucluse", "vendee",
    "vienne", "haute-vienne", "vosges", "yonne", "territoire-de-belfort",
    "essonne", "hauts-de-seine", "seine-saint-denis",
    "val-de-marne", "val-d-oise"
    # DROM
    #"guadeloupe", "martinique", "guyane", "la reunion", "mayotte"
]

##### Fonctions "Asyncronisées" pour la parrallélisation des tâches
async def extractData(context, dep, spe):
    """
    Scrapping de Doctolib pour un département et une spécialité médicale, donnés en argument.
    L'argument context est une instance Playwright (navigateur headless) utilisée pour ouvrir la page.
    Retourne une liste correspondant à une ligne à ajouter au dataframe.
    """
    async with semaphore:
        row = [dep, spe]
        for delai in listeDelais:
            await asyncio.sleep(random.uniform(1, 3))
            try:
                page = await context.new_page()
                await page.set_extra_http_headers({"User-Agent": random.choice(USER_AGENTS)})
                await page.goto(baseUrl + "?speciality=" + spe + "&location=" + dep + "&availabilitiesBefore=" + delai + argsSecteurs, wait_until="domcontentloaded", timeout=120000)
                #if "wait" in await page.content():
                #    await page.wait_for_timeout(random.randint(3000, 8000))  # Wait 3-8 seconds
                #    await page.reload()
                #    await page.wait_for_timeout(60000)
                
                results = page.locator('[data-test-id="hcp-results"]').locator('p').first

                textResultats = await results.inner_text()
                if "Aucun" in textResultats:
                    effectif = 0
                else:
                    effectif = int(textResultats.split(" ")[0])

                await page.close()

            except Exception as e:
                # Prend un screen et envoit un message d'erreur dans la console en cas d'erreur de chargement d'une page
                print(f"Error : {type(e).__name__}: {e}")
                if 'page' in locals():
                    await page.screenshot(path="errors.png")

                effectif = 9999

            row = row + [effectif]

        return row

async def main(cheminCSV):
    """
    Fonction qui sauvegarde les données Doctolib dans un fichier csv
    (les disponibilités par départements).

    Gestion du web-scrapping avec Playwright : 
    - création du contexte Chrome,
    - création de la liste des tâches à faire (des appels à la fonction extractData sur des pages différentes),
    - lancement des tâches en parrallèle avec asyncio,
    - sauvegarde des données dans un csv
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        await stealth.apply_stealth_async(context)

        data = []
        tasks = [extractData(context, dep, spe) for dep in listeDepartements for spe in listeSpecialites]
        data = await asyncio.gather(*tasks)
        
        await context.close()
        await browser.close()

        df = pd.DataFrame(data, columns=columns)
        df.to_csv(cheminCSV, index=False)
                    
    return 1

asyncio.run(main(PATH))