# pip install pytest-playwright
# playwright install
# sudo playwright install-deps

from playwright.sync_api import sync_playwright, expect
from playwright_stealth import stealth
import pandas as pd
import datetime, random, time

## https://www.doctolib.fr/ivg-medicamenteuse/paris?availabilitiesBefore=90&regulationSector%5B%5D=CONTRACTED_1&regulationSector%5B%5D=CONTRACTED_1_WITH_EXTRA&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED_2&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED&regulationSector%5B%5D=CONTRACTED_WITH_EXTRA&regulationSector%5B%5D=ORGANIZATION_CONTRACTED

columns = ["departement", "specialite", "trimestre", "mois", "deuxSemaines"]

baseUrl = "https://www.doctolib.fr/search"
argsSecteurs = "&regulationSector%5B%5D=CONTRACTED_1&regulationSector%5B%5D=CONTRACTED_1_WITH_EXTRA&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED_2&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED&regulationSector%5B%5D=CONTRACTED_WITH_EXTRA&regulationSector%5B%5D=ORGANIZATION_CONTRACTED"

listeDepartements = ["Ile-de-France", "Bouche-du-Rhone", "Ain"]
listeSpecialites = ["ivg-chirurgicale", "ivg-medicale-et-chirurgicale", "ivg-medicamenteuse"]
listeDelais = ["90", "30", "14"]

def getUrl(departement, specialite, delai):
    return baseUrl + "?location=" + departement + "&availabilitiesBefore=" + delai + "&speciality=" + specialite + argsSecteurs

def extractData(context, url):
    page = context.new_page()
    stealth(page)
    page.goto(url)

    print(page.content())

    results = page.locator('[data-test-id="hcp-results"]')
    expect(results).to_have_text()
    # results.locator('p').first.wait_for()

    textResultats = results.locator('p').first.inner_text()
    print(textResultats)
    effectif = int(textResultats.split(" ")[0])
    page.close()

    return effectif

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        matrix = []

        for dep in listeDepartements:
            for i_spe, spe in enumerate(listeSpecialites):
                row = [dep, spe]

                for delai in listeDelais:
                    time.sleep(random.uniform(1, 3))
                    url = getUrl(dep, spe, delai)

                    try:
                        effectif = extractData(context, url)
                    except:
                        effectif = 9999
                    row = row + [effectif]

                matrix.append(row)
                print(row)
            
        context.close()
        browser.close()
        df = pd.DataFrame(matrix, columns=columns)
        
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        
        df.to_csv(date + '.csv', index=False)
                    
    return 1

main()