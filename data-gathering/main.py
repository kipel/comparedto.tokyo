import csv
import json
import os.path
from collections import namedtuple


Country = namedtuple("Country", "name alpha2 alpha3 capital area \
                     population population_year gdp gdp_year")
City = namedtuple("City", "name area population population_year \
                  gdp gdp_year is_capital")


def generate_countries_info():
    '''
        Return basic information about a country and its capital.
        Because there are only 248 countries we can live storing
        all partial info single into lists.

        New country's info will be added into this function, with a specific
        function being responsible for parsing the data file with that info.
        This function will be long and tedious but easy to add or remove
        info by simply adding a call to a parsing function and updating
        the corresponding field in the Country namedtuple.
    '''

    basedir = os.path.join(os.path.dirname(__file__), "raw_data")

    path = os.path.join(basedir, "countries.json")
    countries = loadCountries(path)

    population_year = 2015
    path = os.path.join(basedir, "WPP2015_DB02_Populations_Annual.csv")
    country_population = loadPopulation(path, population_year)

    capitals = [country.capital.name.lower() for country in countries]
    path = os.path.join(basedir, "UNdata_Export_20160106_062450937.csv")
    capitals_population = load_city_population(path, capitals, verbose=False)

    path = os.path.join(basedir, "worldbank_GDP_2015.csv")
    countries_gdp = loadGdp(path)

    for country in countries:
        population = 0
        country_population_year = 0
        country_gdp = 0
        country_gdp_year = 0

        if country.name in country_population.keys():
            population = country_population[country.name]
            country_population_year = population_year

        if country.alpha3 in countries_gdp.keys():
            country_gdp = countries_gdp[country.alpha3]
            country_gdp_year = 2014

        capital = country.capital.name.lower()
        capital_population = capitals_population[capital][0]
        capital_population_year = capitals_population[capital][1]
        updated_capital = country.capital._replace(
                                    population=capital_population,
                                    population_year=capital_population_year)

        yield country._replace(population=population,
                               population_year=country_population_year,
                               gdp=country_gdp,
                               gdp_year=country_gdp_year,
                               capital=updated_capital)


def loadCountries(path):
    with open(path, 'r') as countries:
        countries = json.load(countries)
        country_list = []
        for country in countries:
            capital = City(country["capital"], 0, 0, 0, 0, 0, True)
            c = Country(country["name"]["common"],
                        country["cca2"],
                        country["cca3"],
                        capital,
                        0, 0, 0, 0, 0)
            country_list.append(c)

    return country_list


def loadPopulation(path, year):
    with open(path, 'r', encoding='utf-8') as csv_input:
        data = csv.reader(csv_input, dialect='excel')
        population = [(common_country_name(row[1]),
                      int(float(row[8]) * 1000))
                      for row in data if (row[4] == str(year))]

    return dict(population)


def common_country_name(country):
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


def load_city_population(path, cities_list, verbose=False):
    with open(path, 'r') as csv_input:
        cities = csv.DictReader(csv_input)
        population = []
        previous_city = ""
        for city in cities:
            city_name = city["City"].lower()
            if (city_name in cities_list and
                    city["Sex"].lower() == "both sexes" and
                    previous_city != city_name):
                # Because population values are already ordered from newest
                # to oldest, we can skip checking what line has the latest
                # information.
                # TODO: be vigilant that in future versions this is true.
                previous_city = city_name
                population.append((city_name,
                                  (round(float(city["Value"])),
                                   int(city["Source Year"]))))

        cities_listed = [c[0] for c in population]
        for city in set(cities_list) - set(cities_listed):
            population.append((city.lower(), (0, 0)))

        if verbose:
            print(set(cities_list) - set(cities_listed))

    return dict(population)


def loadGdp(path):
    with open(path, 'r') as csv_input:
        data = csv.reader(csv_input)
        gdp = [(row[0], int(row[4].replace(r',', "").strip())) for row in data]

        return dict(gdp)


def export_json(filename, countries):
    with open(filename, 'w') as world:
        # TODO: refactor JSONEncoder using subclass to serialize obj
        # Quick hack to have Capital obj serialized to json with key : value
        # json.dump doesn't print key if Capital is not given as dict
        with_capital_dict = map(lambda c: c._replace(
                                          capital=c.capital._asdict()),
                                countries)
        countries_as_dict = [c._asdict() for c in with_capital_dict]
        json.dump(countries_as_dict, world, sort_keys=True, indent=2)


if __name__ == "__main__":
    countries = [c for c in generate_countries_info()]
    filename = os.path.join(os.path.dirname(__file__), "json", "world.json")
    export_json(filename, countries)
