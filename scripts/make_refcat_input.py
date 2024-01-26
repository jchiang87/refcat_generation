import os
from collections import defaultdict
import multiprocessing
import numpy as np
import pandas as pd
from refcat_generation import MagErrors, RADecErrors
from skycatalogs import skyCatalogs
from skycatalogs import load_lsst_bandpasses

lsst_bps = load_lsst_bandpasses()

ra, dec = 250., 2.
radius = 0.5
region = skyCatalogs.Disk(ra, dec, radius*3600.0)
mjd = 60390.3293054128
obj_types = {'star'}

skycatalog_yaml = ('/sdf/data/rubin/shared/ops-rehearsal-3/imSim_catalogs/'
                   'skyCatalogs/skyCatalog.yaml')

sky_cat = skyCatalogs.open_catalog(skycatalog_yaml,
                                   skycatalog_root=os.path.dirname(skycatalog_yaml))
objects = sky_cat.get_objects_by_region(region, mjd=mjd,
                                        obj_type_set=obj_types)
nobj = len(objects)
print("# skyCatalog objects:", nobj)

# Baseline cut before simulating uncertainties and applying final cut
# of r_mag > 21.
r_mag_max = 23.

mag_errors = MagErrors()
radec_errors = RADecErrors()

def compute_fluxes(imin, imax):
    global objects
    data = defaultdict(list)
    for i, obj in enumerate(objects[imin:imax]):
        data['object_id'].append(obj.id)
        data['ra'].append(obj.ra)
        data['dec'].append(obj.dec)
        sed = obj.get_total_observer_sed()
        for band in "gri":
            bp = lsst_bps[band]
            data[band].append(sed.calculateMagnitude(bp))
    return pd.DataFrame(data)

processes = 16
indexes = np.linspace(0, nobj+1, processes+1, dtype=int)
with multiprocessing.Pool(processes=processes) as pool:
    workers = []
    for imin, imax in zip(indexes[:-1], indexes[1:]):
        args = (imin, imax)
        workers.append(pool.apply_async(compute_fluxes, args))
    pool.close()
    pool.join()
    dfs = [worker.get() for worker in workers]
df = pd.concat(dfs)
df.to_parquet("initial_refcat_mags.parquet")

#
#output_file = "uw_stars_refmags.csv"
#with open(output_file, 'w') as fobj:
#    catalog_line = "object_id,ra,dec"
#    for band in lsst_bps:
#        catalog_line += f",lsst_{band}_mag"
#    fobj.write(catalog_line + "\n")
#
#    for obj in objects[0:20]:
#        catalog_line = f"{obj.id},{obj.ra},{obj.dec}"
#        sed = obj.get_total_observer_sed()
#        mag = {}_
#        for band, bp in lsst_bps.items():
#            mag[band] = sed.calculateMagnitude(bp)
#            catalog_line += f",{mag[band]}"
#        if mag['r'] < r_mag_max:
#            fobj.write(catalog_line + "\n")
