from __future__ import annotations

import csv
import json

import click
import wikipediaapi
from sentence_transformers import SentenceTransformer


@click.group()
@click.pass_context
def test_data_gen(ctx):
    ctx.ensure_object(dict)


@test_data_gen.command()
@click.option("--csv-path", default="tests/modules/data/worldcities.csv")
@click.option("--queries-path", default="tests/modules/data/vss_queries.json")
@click.option("--output-json-path", default="tests/modules/data/city_index.json")
@click.option("--sample-size", default=25)
@click.pass_context
def generate_cities_index_data(
    ctx, csv_path: str, queries_path: str, output_json_path: str, sample_size: int
):
    model = SentenceTransformer(
        "sentence-transformers/all-distilroberta-v1", cache_folder="scripts/.cache"
    )
    wikiapi = wikipediaapi.Wikipedia("en")
    cities = csv.DictReader(open(csv_path))
    cleaned_cities = {}
    seen_countries = set()
    for city in reversed(
        sorted(cities, key=lambda o: float(o["population"]) if o["population"] else 0)
    ):
        name = city["city"].lower()
        page = wikiapi.page(name)
        if page.exists():
            cleaned_cities[name] = {
                "name": name,
                "country": city["country"],
                "iso_tags": [city["iso2"], city["iso3"]],
                "lat": city["lat"],
                "lng": city["lng"],
                "population": city["population"],
                "summary": page.summary,
                "summary_vector": model.encode(page.summary).tolist(),
            }
            seen_countries.add(city["country"])
        if len(cleaned_cities) == sample_size:
            break

    open(output_json_path, "w").write(json.dumps(cleaned_cities))

    queries = json.loads(open(queries_path).read())
    for query in queries:
        query["embedding"] = model.encode(query["text"]).tolist()

    open(queries_path, "w").write(json.dumps(queries))


if __name__ == "__main__":
    test_data_gen()
