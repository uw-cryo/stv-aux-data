import geopandas as gpd
import pandas as pd
from cloudpathlib import S3Client

# explicitly instantiate a client that always uses the local cache
client = S3Client(local_cache_dir="./",
                  no_sign_request=True)

wesm = client.CloudPath("s3://prd-tnm/StagedProducts/Elevation/metadata/WESM.gpkg")
csv = client.CloudPath("s3://prd-tnm/StagedProducts/Elevation/metadata/WESM.csv")

df = pd.read_csv(csv)

# Now will read from ./prd-tnm/StagedProducts/Elevation/metadata/WESM.gpkg
gf = gpd.read_file(wesm,
                   fid_as_index=True,
)
# Ignore Datum for polygons (<Geographic 2D CRS: EPSG:4269> Name: NAD83)
gf = gf.set_crs("EPSG:4326", allow_override=True)
print(gf.info())

# OK, well, let's just save the convex_hulls instead of complex polygons
gf['geometry'] = gf.geometry.convex_hull

# Also save a geojson for quick browsing on GitHub
gf.to_file('WESM-chulls.geojson')

gf.to_parquet('WESM-chulls.geoparquet',
              schema_version='1.1.0',
              write_covering_bbox=True,
)
