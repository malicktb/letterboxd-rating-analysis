"""
enrich.py
---------
Enriches a Letterboxd export (ratings or watchlist) with TMDB + OMDB metadata.
Caches all API responses locally so re-runs are instant.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SWITCHING BETWEEN MODES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Ratings mode (default):
      python enrich.py --tmdb KEY --omdb KEY
      Input:  ratings.csv        Output: enriched_ratings.csv

  Watchlist mode:
      python enrich.py --tmdb KEY --omdb KEY --watchlist
      Input:  watchlist.csv      Output: enriched_watchlist.csv

You can also override the input/output paths manually:
      python enrich.py --tmdb KEY --omdb KEY --input my_file.csv --output out.csv

The cache (cache/tmdb.json and cache/omdb.json) is shared between both modes,
so films already fetched for ratings won't be re-fetched for the watchlist.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import argparse
import csv
import json
import time
import os
import requests

# -- Config -------------------------------------------------------------------

TMDB_BASE = "https://api.themoviedb.org/3"
OMDB_BASE = "http://www.omdbapi.com"
CACHE_DIR = "cache"

# -- Cache helpers -------------------------------------------------------------

def load_cache(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def save_cache(path, data):
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# -- TMDB ----------------------------------------------------------------------

def fetch_tmdb(title, year, api_key, cache):
    key = f"{title}|{year}"
    if key in cache:
        return cache[key]

    params = {
        "api_key": api_key,
        "query": title,
        "year": year,
        "language": "en-US",
        "include_adult": False,
    }
    r = requests.get(f"{TMDB_BASE}/search/movie", params=params, timeout=10)
    r.raise_for_status()
    results = r.json().get("results", [])

    if not results:
        params.pop("year")
        r = requests.get(f"{TMDB_BASE}/search/movie", params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])

    if not results:
        print(f"  [TMDB] No match: {title} ({year})")
        cache[key] = None
        return None

    movie_id = results[0]["id"]
    detail = requests.get(
        f"{TMDB_BASE}/movie/{movie_id}",
        params={"api_key": api_key, "append_to_response": "credits"},
        timeout=10,
    )
    detail.raise_for_status()
    data = detail.json()
    cache[key] = data
    time.sleep(0.25)
    return data

def parse_tmdb(data):
    if not data:
        return {}
    genres   = [g["name"] for g in data.get("genres", [])]
    director = ""
    for person in data.get("credits", {}).get("crew", []):
        if person.get("job") == "Director":
            director = person["name"]
            break
    return {
        "tmdb_id":           data.get("id", ""),
        "tmdb_title":        data.get("title", ""),
        "genres":            "|".join(genres),
        "runtime":           data.get("runtime", ""),
        "release_year":      data.get("release_date", "")[:4] if data.get("release_date") else "",
        "original_language": data.get("original_language", ""),
        "director":          director,
        "tmdb_vote_avg":     data.get("vote_average", ""),
        "tmdb_vote_count":   data.get("vote_count", ""),
        "tmdb_popularity":   data.get("popularity", ""),
        "budget":            data.get("budget", ""),
    }

# -- OMDB ----------------------------------------------------------------------

def fetch_omdb(title, year, api_key, cache):
    key = f"{title}|{year}"
    if key in cache:
        return cache[key]

    params = {"apikey": api_key, "t": title, "y": year, "type": "movie"}
    r = requests.get(OMDB_BASE, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    if data.get("Response") == "False":
        params.pop("y")
        r = requests.get(OMDB_BASE, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

    if data.get("Response") == "False":
        print(f"  [OMDB] No match: {title} ({year})")
        cache[key] = None
        return None

    cache[key] = data
    time.sleep(0.25)
    return data

def parse_omdb(data):
    if not data:
        return {}
    rt_score = ""
    for r in data.get("Ratings", []):
        if r["Source"] == "Rotten Tomatoes":
            rt_score = r["Value"].replace("%", "")
    def clean(val):
        return "" if val in ("N/A", None) else val
    return {
        "imdb_rating": clean(data.get("imdbRating")),
        "imdb_votes":  clean(data.get("imdbVotes", "").replace(",", "")),
        "rt_score":    rt_score,
        "awards":      0 if clean(data.get("Awards")) == "" else 1,
        "country":     clean(data.get("Country", "").split(",")[0].strip()),
    }

# -- Main ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tmdb",      required=True, help="TMDB API key")
    parser.add_argument("--omdb",      required=True, help="OMDB API key")
    parser.add_argument("--watchlist", action="store_true",
                        help="Watchlist mode: reads watchlist.csv, outputs enriched_watchlist.csv")
    parser.add_argument("--input",     default=None,
                        help="Override input file path")
    parser.add_argument("--output",    default=None,
                        help="Override output file path")
    args = parser.parse_args()

    # Resolve input/output based on mode
    if args.watchlist:
        input_file  = args.input  or "watchlist.csv"
        output_file = args.output or "enriched_watchlist.csv"
    else:
        input_file  = args.input  or "ratings.csv"
        output_file = args.output or "enriched_ratings.csv"

    print(f"Mode:   {'watchlist' if args.watchlist else 'ratings'}")
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}\n")

    # Shared cache between both modes -- films already fetched won't be re-fetched
    tmdb_cache_path = os.path.join(CACHE_DIR, "tmdb.json")
    omdb_cache_path = os.path.join(CACHE_DIR, "omdb.json")
    tmdb_cache = load_cache(tmdb_cache_path)
    omdb_cache = load_cache(omdb_cache_path)

    with open(input_file, newline="", encoding="utf-8-sig") as f:
        films = list(csv.DictReader(f))

    print(f"Enriching {len(films)} films...\n")
    rows = []

    for i, film in enumerate(films):
        title = film["Name"]
        year  = film["Year"]
        print(f"[{i+1}/{len(films)}] {title} ({year})")

        tmdb_raw = fetch_tmdb(title, year, args.tmdb, tmdb_cache)
        omdb_raw = fetch_omdb(title, year, args.omdb, omdb_cache)

        row = {
            "letterboxd_title": title,
            "letterboxd_year":  year,
            **parse_tmdb(tmdb_raw),
            **parse_omdb(omdb_raw),
        }

        # Ratings mode only: include personal rating columns
        if not args.watchlist:
            row["rating"] = float(film["Rating"])
            row["liked"]  = 1 if float(film["Rating"]) >= 3.5 else 0

        rows.append(row)
        save_cache(tmdb_cache_path, tmdb_cache)
        save_cache(omdb_cache_path, omdb_cache)

    fieldnames = list(rows[0].keys())
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    matched = sum(1 for r in rows if r.get("tmdb_id"))
    print(f"\nDone. {matched}/{len(rows)} films matched on TMDB.")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()