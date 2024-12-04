"""
Run a PDAL pipeline to turn 3DEP EPT into DSM/DTM rasters

Given an input raster, chop its footprint into smaller tiles and run a PDAL pipeline for each tile
Finally merge tile DSMs into a single DSM

Code from:
https://github.com/uw-cryo/DeepDEM/blob/main/notebooks/0_Download_LIDAR_data.ipynb


Usage:
python pdal_pipeline.py /tmp/1020010042D39D00.browse.tif
pixi run pdal
"""
import pdal
import os
import rasterio
from rasterio.warp import transform_bounds
from pyproj import CRS
from shapely.geometry import Polygon
import pandas as pd
import geopandas as gpd
import json
import requests
from pathlib import Path
#import dask
import concurrent.futures
import tqdm

def return_readers(filename, n_rows = 5, n_cols=5, buffer_value=0, pointcloud_resolution=10):
    """
    This method takes a raster file and finds overlapping 3DEP data. It then returns a series of readers
    corresponding to non overlapping areas that can be used as part of further PDAL processing pipelines
    The method also returns the CRS specified i
    """
    with rasterio.open(filename) as ds:
        src_bounds = ds.bounds
        src_crs = ds.crs
        src_transform = ds.transform

    # Instead set as an argument
    # this assumes the values to be in meters
    #x_resolution, y_resolution = abs(src_transform[0]), src_transform[4]
    # the point cloud resolution will be determined by the
    # coarsest resolution available in our raster data
    #pointcloud_resolution = max([x_resolution, y_resolution])

    xmin, ymin, xmax, ymax = src_bounds
    x_step = (xmax - xmin) / n_cols
    y_step = (ymax - ymin) / n_rows

    dst_crs = CRS.from_epsg(4326)

    readers = []

    for i in range(int(n_cols)):
        for j in range(int(n_rows)):
            aoi = Polygon.from_bounds(xmin+i*x_step, ymin+j*y_step, xmin+(i+1)*x_step, ymin+(j+1)*y_step)

            src_bounds_transformed = transform_bounds(src_crs, dst_crs, *aoi.bounds)
            aoi_4326 = Polygon.from_bounds(*src_bounds_transformed)

            src_bounds_transformed_3857 = transform_bounds(src_crs, CRS.from_epsg(3857), *aoi.bounds)
            aoi_3857 = Polygon.from_bounds(*src_bounds_transformed_3857)
            if buffer_value:
                aoi_3857.buffer(buffer_value)

            # https://github.com/hobuinc/usgs-lidar/blob/master/boundaries/resources.geojson
            gdf = gpd.read_file('https://raw.githubusercontent.com/hobuinc/usgs-lidar/master/boundaries/resources.geojson').set_crs(4326)
            # in the eventuality that the above URL breaks, we store a local copy
            # gdf = gpd.read_file('../data/shapefiles/resources.geojson').set_crs(4326)

            for _, row in gdf.iterrows():
                if row.geometry.intersects(aoi_4326):
                    usgs_dataset_name = row['name']
                    break

            url = f"https://s3-us-west-2.amazonaws.com/usgs-lidar-public/{usgs_dataset_name}/ept.json"
            reader = {
            "type": "readers.ept",
            "filename": url,
            "resolution": pointcloud_resolution,
            "polygon": str(aoi_3857.wkt),
            }

            # SRS associated with the 3DEP dataset
            response = requests.get(url)
            data = response.json()
            srs_wkt = data['srs']['wkt']
            pointcloud_input_crs = CRS.from_wkt(srs_wkt)

            readers.append(reader)

    return readers, pointcloud_input_crs


# function that returns a PDAL pipeline to create a pointcloud based on user flags
def create_pdal_pipeline(filter_low_noise=True, filter_high_noise=True,
                         filter_road=True, reset_classes=False, reclassify_ground=False,
                         return_only_ground=False, percentile_filter=True, percentile_threshold=0.95,
                         reproject=True, save_pointcloud=False,
                         pointcloud_file = 'pointcloud', input_crs=None,
                         output_crs=None, output_type='laz'):

    assert abs(percentile_threshold) <= 1, "Percentile threshold must be in range [0, 1]"
    assert output_type in ['las', 'laz'], "Output type must be either 'las' or 'laz'"
    assert output_crs is not None, "Argument 'output_crs' must be explicitly specified!"

    stage_filter_low_noise = {
        "type":"filters.range",
        "limits":"Classification![7:7]"
    }
    stage_filter_high_noise = {
        "type":"filters.range",
        "limits":"Classification![18:18]"
    }
    stage_filter_road = {
        "type":"filters.range",
        "limits":"Classification![11:11]"
    }
    stage_reset_classes = {
        "type":"filters.assign",
        "value":"Classification = 0"
    }
    stage_reclassify_ground = {
        "type":"filters.smrf",
        # added from pdal smrf documentation, in turn from Pingel, 2013
        "scalar":1.2,
        "slope":0.2,
        "threshold":0.45,
        "window":8.0
    }
    stage_percentile_filter =  {
        "type":"filters.python",
        "script":"filter_percentile.py",
        "pdalargs": {"percentile_threshold":percentile_threshold},
        "function":"filter_percentile",
        "module":"anything"
    }
    stage_return_ground = {
        "type":"filters.range",
        "limits":"Classification[2:2]"
    }

    stage_reprojection = {
        "type":"filters.reprojection",
        "out_srs":str(output_crs)
    }
    if input_crs is not None:
        stage_reprojection["in_srs"] = str(input_crs)

    stage_save_pointcloud_las = {
        "type": "writers.las",
        "filename": f"{pointcloud_file}.las"
    }
    stage_save_pointcloud_laz = {
        "type": "writers.las",
        "compression": "true",
        "minor_version": "2",
        "dataformat_id": "0",
        "filename": f"{pointcloud_file}.laz"
    }

    # Build pipeline
    pipeline = []

    # resetting the original classifications resets
    # all point classifications to 0 (Unclassified)
    if reset_classes:
        pipeline.append(stage_reset_classes)
        if reclassify_ground:
            pipeline.append(stage_reclassify_ground)
    else:
        # we apply the percentile filter first as it
        # classifies detected outliers as 'high noise'
        if percentile_filter:
            pipeline.append(stage_percentile_filter)
        if filter_low_noise:
            pipeline.append(stage_filter_low_noise)
        if percentile_filter or filter_high_noise:
            pipeline.append(stage_filter_high_noise)
        if filter_road:
            pipeline.append(stage_filter_road)


    # For creating DTMs, we want to process only ground returns
    if return_only_ground:
        pipeline.append(stage_return_ground)

    if reproject:
        pipeline.append(stage_reprojection)

    # the pipeline can save the pointclouds to a separate file if needed
    if save_pointcloud:
        if output_type == 'laz':
            pipeline.append(stage_save_pointcloud_laz)
        else:
            pipeline.append(stage_save_pointcloud_las)

    return pipeline


# function that returns a PDAL pipeline to create a DEM based on user flags
def create_dem_stage(dem_filename='dem_output.tif', pointcloud_resolution=10,
                        gridmethod='idw', dimension='Z'):
    dem_stage = {
            "type":"writers.gdal",
            "filename":dem_filename,
            "gdaldriver":'GTiff',
            "nodata":-9999,
            "output_type":gridmethod,
            "resolution":float(pointcloud_resolution),
            "gdalopts":"COMPRESS=LZW,TILED=YES,blockxsize=256,blockysize=256,COPY_SRC_OVERVIEWS=YES"
    }

    if dimension == 'Z':
        dem_stage.update({
            'dimension': 'Z',
            'where': 'Z>0'
        })
    else:
        dem_stage.update({
            'dimension':dimension
        })

    return [dem_stage]


def main():
    # raster file for which pointcloud is generated
    input_file = '/tmp/1020010042D39D00.browse.tif'

    # we use a user specified output srs
    crs_file = 'UTM_10N_WGS84_G2139_3D.wkt'
    with open(crs_file, 'r') as f:
        OUTPUT_CRS = CRS.from_string(f.read())

    # The method returns pointcloud readers, as well as the pointcloud file CRS as a WKT string
    # Specfying a buffer_value > 0 will generate overlapping DEM tiles, resulting in a seamless
    # final mosaicked DEM
    print('Generating readers...')
    readers, POINTCLOUD_CRS = return_readers(input_file, n_rows=4, n_cols=4, buffer_value=100)

    # Set pointcloud processing parameters
    FILTER_LOW_NOISE = True
    FILTER_HIGH_NOISE = True
    FILTER_ROAD = True
    RETURN_ONLY_GROUND = False # Set true for DTM
    RESET_CLASSES = False
    RECLASSIFY_GROUND = False
    PERCENTILE_FILTER = True # Set to True to apply percentile based filtering of Z values
    PERCENTILE_THRESHOLD = 0.95 # Percentile value to filter out noisy Z returns
    REPROJECT = True
    SAVE_POINTCLOUD=False
    POINTCLOUD_RESOLUTION = 10 # in meters, can reduce to 1 once working :)
    OUTPUT_TYPE='laz'
    GRID_METHOD='idw'
    DIMENSION='Z' # can be set to options accepted by writers.gdal. Set to 'intensity' to return intensity rasters

    output_path = Path('/tmp/deepdem')
    output_path.mkdir(exist_ok=True)

    pipelines = []
    for i, reader in enumerate(readers[:4]):
        print(f'Processing tile {i+1} of {len(readers)}...')
        print(reader)
        dem_file = output_path / f'dem_tile_aoi_{str(i).zfill(4)}.tif'
        pipeline = {'pipeline':[reader]}

        pdal_pipeline = create_pdal_pipeline(
            filter_low_noise=FILTER_LOW_NOISE,
            filter_high_noise=FILTER_HIGH_NOISE,
            filter_road=FILTER_ROAD,
            reset_classes=RESET_CLASSES,
            reclassify_ground=RECLASSIFY_GROUND,
            return_only_ground=RETURN_ONLY_GROUND,
            percentile_filter=PERCENTILE_FILTER,
            percentile_threshold=PERCENTILE_THRESHOLD,
            reproject=REPROJECT,
            save_pointcloud=SAVE_POINTCLOUD,
            pointcloud_file='pointcloud',
            input_crs = POINTCLOUD_CRS,
            output_crs=OUTPUT_CRS,
            output_type=OUTPUT_TYPE
        )

        dem_stage = create_dem_stage(dem_filename=str(dem_file),
                                        pointcloud_resolution=POINTCLOUD_RESOLUTION,
                                        gridmethod=GRID_METHOD, dimension=DIMENSION)

        # apply interpolation to fill gaps when generating DTM
        if RETURN_ONLY_GROUND:
            dem_stage[0]['window_size'] = 4

        pipeline['pipeline'] += pdal_pipeline
        pipeline['pipeline'] += dem_stage
        #print(json.dumps(pipeline))
        pipeline = pdal.Pipeline(json.dumps(pipeline))
        pipelines.append(pipeline)
        #pipeline.execute()

    # Parallelize
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(pipeline.execute) for pipeline in pipelines]
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            print(future.result())

    # merge rasters
    if RETURN_ONLY_GROUND:
        merged_filename = output_path.parent / 'merged_dtm.tif'
    else:
        merged_filename = output_path.parent / 'merged_dsm.tif'

    #os.system(f"dem_mosaic -o {str(merged_filename)} {str(output_path)}/*.tif")
    os.system(f"gdalbuildvrt mosaic.vrt {str(output_path)}/*.tif")

    # delete temporary files
    #for file in list(output_path.glob('*.tif')):
    #    file.unlink()


if __name__ == '__main__':
    main()
