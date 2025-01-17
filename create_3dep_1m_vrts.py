"""
Generate per-project VRTs for 3DEP 1m tiles

Check outputs:
GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR gdalinfo 3DEM_1m/Fork_Salt_River_NRCS/Fork_Salt_River_NRCS_UTM15.vrt

# NOTE: todo, some have heterogeneous projections
0...10...20..Warning 1: gdalbuildvrt does not support heterogeneous projection: expected NAD83 / UTM zone 19N, got NAD83 / UTM zone 20N. Skipping /vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/PR_PRVI_F_2018/TIFF/USGS_1m_x13y204_PR_PRVI_F_2018.tif
.30...40...50...60...70...80...90.Warning 1: gdalbuildvrt does not support heterogeneous projection: expected NAD83 / UTM zone 19N, got NAD83 / UTM zone 20N. Skipping /vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/PR_PRVI_F_2018/TIFF/USGS_1m_x13y205_PR_PRVI_F_2018.tif
..100 - done.
"""
import geopandas as gpd
import requests
import os
from pathlib import Path

# url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/FullExtentSpatialMetadata/FESM_1m.gpkg'
df = gpd.read_file('FESM_1m.gpkg', ignore_geometry=True)

for i,row in df.iterrows():
    print(i, row.project)
    outdir = Path(f'3DEP_1m/{row.project}')
    outdir.mkdir(parents=True, exist_ok=True)
    if len(list(outdir.glob('*.vrt'))) >= 1:
        print('VRTs already exist, skipping...')
    else:
        tif_list = row.product_link.replace('index.html?prefix=','') + '/0_file_download_links.txt'
        r = requests.get(tif_list)
        tif_urls = r.text.split()
        # First pass: Assume naming convention starts w/ UTM Zone: USGS_1M_15_x61y434_Fork_Salt_River_NRCS.tif
        # NOTE: this was False, see
        #https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/DE_Snds_2013/TIFF/USGS_one_meter_x49y430_DE_Snds_2013.tif
        # Warning 1: gdalbuildvrt does not support heterogeneous projection: expected NAD83 / UTM zone 14N, got NAD83 / UTM zone 15N. Skipping /vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/TX_Neches_B5_2016/TIFF/USGS_one_meter_x20y356_TX_Neches_B5_2016.tif
        # So it seems the only way to accurately create all the VRTS is to 
        # read all the Tifs
        # This is where having STAC metadata + GTI also simplifies things
        # (Create STAC Items, either mash into single GTI or keep separate for browsing)
        utm_zones = set([os.path.basename(x).split('_')[2] for x in tif_urls])
        try:
            utm_zones = [int(zone) for zone in utm_zones]
            for zone in utm_zones:
                output_vrt = Path(outdir, f'{row.project}_UTM{zone}.vrt')  
                gdal_paths = [f'/vsicurl/{x}' for x in tif_urls if f'1M_{zone}_' in x]
                gdal_string = ' '.join(gdal_paths)
                cmd = f'GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR gdalbuildvrt {output_vrt} {gdal_string}'
                os.system(cmd)
        except:
            # Just assume single UTM Zone
            output_vrt = Path(outdir, f'{row.project}_UTM.vrt') 
            gdal_paths = [f'/vsicurl/{x}' for x in tif_urls]
            gdal_string = ' '.join(gdal_paths)
            cmd = f'GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR gdalbuildvrt {output_vrt} {gdal_string}'
            os.system(cmd)            