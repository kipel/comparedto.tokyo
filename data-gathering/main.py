import csv
import json
import os.path

#from bs4 import BeautifulSoup


def generate_countries_info():
    basedir = os.path.join(os.path.dirname(__file__), "raw_data")

    population_year = 2015
    path = os.path.join(basedir, "WPP2015_DB02_Populations_Annual.csv")
    country_population = loadPopulation(path, population_year)

    countries_gdp = loadGdp(os.path.join(basedir, "worldbank_GDP_2015.csv"))

    path = os.path.join(basedir, "countries.json")
    with open(path, 'r') as countries:
        countries = json.load(countries)
        for country in countries:
            capital = {"name": country["capital"],
                       "area": 0,
                       "population": 0,
                       "population_year": 0,
                       "gdp": 0,
                       "gdp_year": 0,
                       "is_capital": True}

            name = country["name"]["common"]
            alpha2 = country["cca2"]
            alpha3 = country["cca3"]

            area = 0

            if name in country_population.keys():
                population = country_population[name]
            else:
                population = 0
                population_year = 0

            if alpha3 in countries_gdp.keys():
                gdp = countries_gdp[alpha3]
                gdp_year = 2014
            else:
                gdp = 0
                gdp_year = 0

            yield {"name": name, "area": area, "population": population,
                   "population_year": population_year, "gdp": gdp,
                   "gdp_year": gdp_year, "alpha2": alpha2,
                   "alpha3": alpha3, "cities": [capital]}


def export_json(filename, countries):
    with open(filename, 'w') as world:
        json.dump(countries, world, sort_keys=True, indent=2)


def loadPopulation(path, year):
    with open(path, 'r', encoding='utf-8') as csv_input:
        data = csv.reader(csv_input, dialect='excel')
        population = [(common_name(row[1]),
                      int(float(row[8]) * 1000))
                      for row in data if (row[4] == str(year))]

    return dict(population)


def common_name(country):
    from collections import defaultdict
    '''
        Returns the common name used in countries.json.
        @params: Country name used by the UN World Population prospects
    '''

    common = defaultdict(lambda: country)

    common["Bolivia (Plurinational State of)"] = "Bolivia"
    common["Brunei Darussalam"] = "Brunei"
    common["Cote d'Ivoire"] = "Ivory Coast"
    common["Democratic Republic of the Congo"] = "DR Congo"
    common["Congo"] = "Republic of the Congo"
    common["Cabo Verde"] = "Cape Verde"
    common["China, Hong Kong SAR"] = "Hong Kong"
    common["Iran (Islamic Republic of)"] = "Iran"
    common["Republic of Korea"] = "South Korea"
    common["Republic of Moldova"] = "Moldova"
    common["TFYR Macedonia"] = "Macedonia"
    common["Dem. People's Republic of Korea"] = "North Korea"
    common["State of Palestine"] = "Palestine"
    common["Russian Federation"] = "Russia"
    common["Syrian Arab Republic"] = "Syria"
    common["United Republic of Tanzania"] = "Tanzania"
    common["United States of America"] = "United States"
    common["Venezuela (Bolivarian Republic of)"] = "Venezuela"
    common["Viet Nam"] = "Vietnam"

    return common[country]


def loadGdp(path):
    with open(path, 'r') as csv_input:
        data = csv.reader(csv_input)
        gdp = [(row[0], row[4].replace(r',', "").strip()) for row in data]

        return dict(gdp)


if __name__ == "__main__":
    countries = [c for c in generate_countries_info()]
    filename = os.path.join(os.path.dirname(__file__), "json", "world.json")
    export_json(filename, countries)
