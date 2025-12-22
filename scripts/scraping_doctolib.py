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
columns = ["code_dep", "specialite", "trimestre", "mois", "deuxSemaines"]
baseUrl = "https://www.doctolib.fr/search"
argsSecteurs = "&regulationSector%5B%5D=CONTRACTED_1&regulationSector%5B%5D=CONTRACTED_1_WITH_EXTRA&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED_2&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED&regulationSector%5B%5D=CONTRACTED_WITH_EXTRA&regulationSector%5B%5D=ORGANIZATION_CONTRACTED"
listeSpecialites = ["ivg-chirurgicale", "ivg-medicale-et-chirurgicale", "ivg-medicamenteuse"]
listeDelais = ["90", "30", "14"]
dictionnaire_departements = {
    "ain": "01","aisne": "02","allier": "03","alpes-de-haute-provence": "04","hautes-alpes": "05",
    "alpes-maritimes": "06","ardeche": "07","ardennes": "08","ariege": "09","aube": "10",
    "aude": "11","aveyron": "12","bouches-du-rhone": "13","calvados": "14","cantal": "15",
    "charente": "16","charente-maritime": "17","cher": "18","correze": "19",
    "corse-du-sud": "2A","haute-corse": "2B","cote-d-or": "21","cotes-d-armor": "22",
    "creuse": "23","dordogne": "24","doubs": "25","drome": "26","eure": "27","eure-et-loir": "28",
    "finistere": "29","gard": "30","haute-garonne": "31","gers": "32","gironde": "33","herault": "34",
    "ille-et-vilaine": "35","indre": "36","indre-et-loire": "37","isere": "38","jura": "39",
    "landes": "40", "loir-et-cher": "41","loire": "42","haute-loire": "43","loire-atlantique": "44",
    "loiret": "45","lot": "46","lot-et-garonne": "47","lozere": "48","maine-et-loire": "49","manche": "50",
    "marne": "51","haute-marne": "52","mayenne": "53","meurthe-et-moselle": "54","meuse": "55",
    "morbihan": "56","moselle": "57","nievre": "58","nord": "59","oise": "60","orne": "61",
    "pas-de-calais": "62","puy-de-dome": "63","pyrenees-atlantiques": "64","hautes-pyrenees": "65",
    "pyrenees-orientales": "66","bas-rhin": "67","haut-rhin": "68","rhone": "69","haute-saone": "70",
    "saone-et-loire": "71","sarthe": "72","savoie": "73","haute-savoie": "74","paris": "75","seine-maritime": "76",
    "seine-et-marne": "77","yvelines": "78","deux-sevres": "79","somme": "80","tarn": "81","tarn-et-garonne": "82",
    "var": "83","vaucluse": "84","vendee": "85","vienne": "86","haute-vienne": "87","vosges": "88",
    "yonne": "89","territoire-de-belfort": "90","essonne": "91","hauts-de-seine": "92",
    "seine-saint-denis": "93","val-de-marne": "94","val-d-oise": "95"
    # "guadeloupe", "martinique", "guyane", "la reunion", "mayotte"
}

##### Fonctions "Asyncronisées" pour la parrallélisation des tâches
async def extractData(context, dep, spe):
    """
    Scrapping de Doctolib pour un département et une spécialité médicale, donnés en argument.
    L'argument context est une instance Playwright (navigateur headless) utilisée pour ouvrir la page.
    Retourne une liste correspondant à une ligne à ajouter au dataframe.
    """
    async with semaphore:
        row = [dictionnaire_departements[dep], spe]
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
        tasks = [extractData(context, dep, spe) for dep in dictionnaire_departements.keys() for spe in listeSpecialites]
        data = await asyncio.gather(*tasks)
        
        await context.close()
        await browser.close()

        df = pd.DataFrame(data, columns=columns)
        df.to_csv(cheminCSV, index=False)
                    
    return 1

asyncio.run(main(PATH))