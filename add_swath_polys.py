import geopandas as gpd
import pandas as pd
from cloudpathlib import S3Client

# explicitly instantiate a client that always uses the local cache
client = S3Client(no_sign_request=True)

def get_swath_poly(metadata_link):
    path = client.CloudPath('s3://prd-tnm/'+metadata_link.split('prefix=')[1])
    shapefiles = list(path.rglob('spatial_metadata/USGS/**/*.shp'))
    try:
        swath_poly_key = next(str(x) for x in shapefiles if "swath" in str(x).lower())
        return swath_poly_key
    except StopIteration:
        print(metadata_link)
        return None

# very slow... but only need to run once hopefully...
#df['swath_poly'] = df['metadata_link'].apply(get_swath_poly)

# Just load results from previous run and save to GeoJSON and GeoParquet
df = pd.read_csv('results.csv')
gf = gpd.read_file('WESM-chulls.geojson')
gf['swath_poly'] = df.swath_poly
gf.to_file('WESM-chulls.geojson')
