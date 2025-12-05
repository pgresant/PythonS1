##### Installation for the dependancies
# pip install pytest-playwright
# sudo playwright install-deps
# playwright install

##### Dependancies
from playwright.async_api import async_playwright, expect
from playwright_stealth import Stealth
import pandas as pd
import datetime, random, time
import asyncio

##### Variables : URL Parameters & dataframe columns
columns = ["departement", "specialite", "trimestre", "mois", "deuxSemaines"]

baseUrl = "https://www.doctolib.fr/search"
argsSecteurs = "&regulationSector%5B%5D=CONTRACTED_1&regulationSector%5B%5D=CONTRACTED_1_WITH_EXTRA&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_1_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED_2&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM&regulationSector%5B%5D=CONTRACTED_2_WITH_OPTAM_CO&regulationSector%5B%5D=CONTRACTED&regulationSector%5B%5D=CONTRACTED_WITH_EXTRA&regulationSector%5B%5D=ORGANIZATION_CONTRACTED"

listeDepartements = ["Ile-de-France", "Bouche-du-Rhone", "Ain"]
listeSpecialites = ["ivg-chirurgicale", "ivg-medicale-et-chirurgicale", "ivg-medicamenteuse"]
listeDelais = ["90", "30", "14"]

##### Variables : options for Playwright and semaphore for asyncio
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
]
semaphore = asyncio.Semaphore(10)
custom_languages = ("fr-FR", "fr")
stealth = Stealth(
    navigator_languages_override=custom_languages
)

##### Function to load a url & parse the data
async def extractData(p, dep, spe):
    async with semaphore:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await stealth.apply_stealth_async(context)

        row = [dep, spe]
        for delai in listeDelais:
            await asyncio.sleep(random.uniform(1, 3))
            url = baseUrl + "?location=" + dep + "&availabilitiesBefore=" + delai + "&speciality=" + spe + argsSecteurs

            try:
                page = await context.new_page()
                await page.set_extra_http_headers({
                    "User-Agent": random.choice(USER_AGENTS)
                })
                await page.goto(url, wait_until="domcontentloaded", timeout=120000)
                if "wait" in await page.content():
                    await page.wait_for_timeout(random.randint(3000, 8000)) 
                    await page.reload()
                    await page.wait_for_timeout(60000)

                results = await page.locator('[data-test-id="hcp-results"]')
                expect(results).to_have_text()
                # results.locator('p').first.wait_for()

                textResultats = await results.locator('p').first.inner_text()
                print(textResultats)
                effectif = await int(textResultats.split(" ")[0])

                await page.close()
            except:
                effectif = 9999
            
            row = row + [effectif]
        
        await context.close()
        await browser.close()
        return row

##### Function to 
async def main():
    async with async_playwright() as p:
        matrix = []
        tasks = [extractData(p, dep, spe) for dep in listeDepartements for spe in listeSpecialites]
        matrix = await asyncio.gather(*tasks)
            
        df = pd.DataFrame(matrix, columns=columns)
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        df.to_csv(date + '.csv', index=False)
                    
    return 1

asyncio.run(main())