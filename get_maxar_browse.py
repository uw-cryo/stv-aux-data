"""
Usage:
python get_maxar_browse.py 102001008EC5AC00
pixi run get_maxar 1020010042D39D00
"""
import coincident
import argparse
import asyncio
import rich.table
from rich import print as rprint

def parse_args():
    parser = argparse.ArgumentParser(description='Get Maxar browse image.')
    parser.add_argument('id', metavar='ID', type=str, help='ID string')
    return parser.parse_args()

args = parse_args()

gf = coincident.search.search(dataset='maxar',
                         ids=[args.id],
)

items = coincident.search.stac.to_pystac_items(gf)

# Nice display of STAC metadata
t_metadata = rich.table.Table("Key", "Value")
for k, v in sorted(items[0].properties.items()):
    t_metadata.add_row(k, str(v))

rprint(t_metadata)

print(f'Downloading to /tmp/{args.id}.browse.tif...')
asyncio.run(coincident.datasets.maxar.download_item(items[0]))
