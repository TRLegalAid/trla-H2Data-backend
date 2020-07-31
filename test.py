import pandas as pd
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")

x = client.geocode("11 Cortlandt Manor Rd Katonah NY 10536")
z = x["results"][0]["accuracy_type"]
z = x["results"][0]['location']['lat'], x["results"][0]['location']['lng']

print(type(z))
print(z)
