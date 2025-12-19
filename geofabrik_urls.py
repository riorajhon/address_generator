#!/usr/bin/env python3
"""
Geofabrik URL mapping for OSM PBF files
Maps country codes to correct Geofabrik download URLs
"""

# Complete Geofabrik URL mapping based on official structure
GEOFABRIK_URLS = {
    #inserted
    'AG': 'https://download.openstreetmap.fr/extracts/central-america/antigua_and_barbuda-latest.osm.pbf',
    'AI': 'https://download.openstreetmap.fr/extracts/central-america/anguilla-latest.osm.pbf',
    'AM': 'https://download.geofabrik.de/asia/armenia-latest.osm.pbf',
    'AQ': 'https://download.geofabrik.de/antarctica-latest.osm.pbf',
    'AS': 'https://download.geofabrik.de/australia-oceania/samoa-latest.osm.pbf',
    'AW': 'https://download.openstreetmap.fr/extracts/central-america/aruba-latest.osm.pbf',
    'AX': 'https://download.openstreetmap.fr/extracts/europe/finland/aland-latest.osm.pbf',
    'AZ': 'https://download.geofabrik.de/asia/azerbaijan-latest.osm.pbf',
    'BB': 'https://download.openstreetmap.fr/extracts/central-america/barbados-latest.osm.pbf',
    'BL': 'https://download.openstreetmap.fr/extracts/central-america/saint_barthelemy-latest.osm.pbf',
    'BM': 'https://download.geofabrik.de/europe//united-kingdom/bermuda-latest.osm.pbf',
    'BN': 'https://download.openstreetmap.fr/extracts/asia/brunei-latest.osm.pbf',
    'BS': 'https://download.geofabrik.de/central-america/bahamas-latest.osm.pbf',
    'BV': 'https://geo2day.com/africa/bouvet_island.pbf',
    'CA': 'https://download.geofabrik.de/north-america/canada-251218.osm.pbf',
    'CC': 'https://download.openstreetmap.fr/extracts/oceania/australia/cocos_islands-latest.osm.pbf',
    'CK': 'https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf',
    'CU': 'https://download.geofabrik.de/central-america/cuba-latest.osm.pbf',
    'CW': 'https://download.openstreetmap.fr/extracts/central-america/curacao-latest.osm.pbf',
    'CX': 'https://download.openstreetmap.fr/extracts/oceania/australia/christmas_island-latest.osm.pbf',
    'DM': 'https://download.openstreetmap.fr/extracts/central-america/dominica-latest.osm.pbf',
    'DO': 'https://download.geofabrik.de/central-america/haiti-and-domrep-latest.osm.pbf',
    'FK': 'https://download.geofabrik.de/europe//united-kingdom/falklands-latest.osm.pbf',
    'FM': 'https://download.geofabrik.de/australia-oceania/micronesia-latest.osm.pbf',
    'GD': 'https://download.openstreetmap.fr/extracts/central-america/grenada-latest.osm.pbf',
    'GE': 'https://download.geofabrik.de/europe/georgia-latest.osm.pbf',
    'GF': 'https://download.geofabrik.de/europe/france/guyane-latest.osm.pbf',
    'GG': 'https://download.geofabrik.de/europe/guernsey-jersey-latest.osm.pbf',
    'GI': 'https://download.openstreetmap.fr/extracts/europe/gibraltar-latest.osm.pbf',
    'GL': 'https://download.geofabrik.de/north-america/greenland-latest.osm.pbf',
    'GM': 'https://download.openstreetmap.fr/extracts/africa/gambia-latest.osm.pbf',
    'GP': 'https://download.geofabrik.de/europe/france/guadeloupe-latest.osm.pbf',
    'GS': 'https://download.openstreetmap.fr/extracts/south-america/south_georgia_and_south_sandwich-latest.osm.pbf',
    'GU': 'https://download.openstreetmap.fr/extracts/oceania/guam-latest.osm.pbf',
    'HK': 'https://download.geofabrik.de/asia/china/hong-kong-latest.osm.pbf',
    'HM': 'https://download.openstreetmap.fr/extracts/oceania/australia/heard_island_and_mcdonald_slands.osm.pbf',
    'HT': 'https://download.geofabrik.de/central-america/haiti-and-domrep-latest.osm.pbf',
    'JE': 'https://download.geofabrik.de/europe/guernsey-jersey-latest.osm.pbf',
    'JM': 'https://download.geofabrik.de/central-america/jamaica-latest.osm.pbf',
    'KI': 'https://download.geofabrik.de/australia-oceania/kiribati-latest.osm.pbf',
    'KN': 'https://geo2day.com/central_america/saint_kitts_and_nevis.pbf',
    'KY': 'https://download.openstreetmap.fr/extracts/central-america/cayman_islands-latest.osm.pbf',
    'LC': 'https://download.openstreetmap.fr/extracts/central-america/saint_lucia-latest.osm.pbf',
    'MF': 'https://download.openstreetmap.fr/extracts/central-america/saint_martin-latest.osm.pbf',
    'MH': 'https://download.geofabrik.de/australia-oceania/marshall-islands-latest.osm.pbf',
    'MO': 'https://download.openstreetmap.fr/extracts/asia/china/macau-latest.osm.pbf',
    'MQ': 'https://download.geofabrik.de/europe/france/martinique-latest.osm.pbf',
    'MS': 'https://download.openstreetmap.fr/extracts/central-america/montserrat-latest.osm.pbf',
    'MY': 'https://download.openstreetmap.fr/extracts/asia/malaysia-latest.osm.pbf',
    'NC': 'https://download.geofabrik.de/australia-oceania/new-caledonia-latest.osm.pbf',
    'NF': 'https://download.openstreetmap.fr/extracts/oceania/australia/norfolk_island-latest.osm.pbf'
}

def get_geofabrik_url(country_code: str, country_name: str) -> str:
    """Get the correct Geofabrik URL for a country"""
    country_code = country_code.upper()
    
    # Check if we have a specific mapping
    if country_code in GEOFABRIK_URLS:
        return GEOFABRIK_URLS[country_code]
    
    # Fallback: try to construct URL from country name
    country_slug = country_name.lower().replace(' ', '-').replace('&', 'and')
    return f"https://download.geofabrik.de/{country_slug}-latest.osm.pbf"

if __name__ == "__main__":
    # Test some mappings
    test_countries = [
        ('MC', 'Monaco'),
        ('AD', 'Andorra'), 
        ('US', 'United States'),
        ('DE', 'Germany'),
        ('CN', 'China')
    ]
    
    for code, name in test_countries:
        url = get_geofabrik_url(code, name)
        print(f"{code} ({name}): {url}")