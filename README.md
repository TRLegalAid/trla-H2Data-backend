# trla-H2Data-backend

## background

Python app hosted on Heroku which manages and automatically updates a PostgreSQL database of H-2A and H-2B job listings. Includes processes to geocode addresses, check results for accuracy, and implements a system for people to fix the inaccurate addresses. Most frequent use of this code locally will be to upload new quarterly data and to re-run a script to pull new data.

## how it works

Job postings are added daily from a web scraper hosted on [Apify](https://my.apify.com/account), which gets latest postings from [here](https://seasonaljobs.dol.gov/). Each quarter, the official dataset put out by the US Department of Labor is merged with the existing data. 

Frontend web-app to visualize, filter, and download the data is [here](https://trla.shinyapps.io/H2Data/). Frontend code is [located here](https://github.com/TRLegalAid/trla-h2data-frontend-R).

## uploading new quarterly data

See [this file](https://txriogrande.sharepoint.com/:w:/r/sites/DataMapsTRLA2/_layouts/15/Doc.aspx?sourcedoc=%7BFDB851BC-008A-4D01-8B94-338A837C7319%7D&file=Adding%20Quarterly%20Data.docx&nav=eyJjIjoyMjM0NDAyODN9&action=default&mobileredirect=true&cid=f7ba92ed-7a24-4370-8d4e-35691b445331) for a detailed explanation.

DOL data can be found online as "performance data" or "quarterly disclosure data," typically [found here](https://www.dol.gov/agencies/eta/foreign-labor/performance). The disclosure data has many more variables than we are able to scrape and includes addenda.

## re-running for missed data

If the python script fails, you can re-run the script after the issue has been corrected.

First, go to Apify. Select Actors > Click the name of the actor (apify-dol-actor at time of writing) > Runs > Click the green, hyperlinked "status" of the run you are interested in > Under the 4 dashboard items with results, select "API" > Copy URL under Get Dataset Items

Then, go to the update_database.py file. Temporarily replace "most_recent_run_url" with the URL you got from Apify (the version in line 18, within requests.get().json().

Run the script in your console, and everything should be up to date. If you accidentally add duplicate case numbers, it's okay. They will not actually end up in the database.

## cleaning up data 

Data is cleaned via [this google doc](https://docs.google.com/spreadsheets/d/1qNK57DTebJstUwMyZBH3cc_5mMrvTJm1ZgiKGn2F2kg/edit#gid=0). TRLA staff only cleans data for TX, AR, LA, KY, AL, MS, and TN.

## issues with the scraper itself

If the DOL website format changes, you may need to alter the scraper. This may also be the case if you want to attempt to scrape more fields.

We have contracted with a developer at Apify to create the scraper. You can send new requests for changes, and the developer will let you know a one-time cost. 

You will just submit a new request through Apify's marketplace. Hopefully, if the original developer is still providing services through the platform, we can get connected to the same developer as before.
