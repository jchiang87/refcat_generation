import os
import glob
import sys
from collections import defaultdict
import multiprocessing
import warnings
import numpy as np
import pandas as pd
from refcat_generation import MagErrors, RADecErrors
from skycatalogs import skyCatalogs
from skycatalogs import load_lsst_bandpasses


lsst_bps = load_lsst_bandpasses()


def compute_mags(imin, imax, lsst_bps=lsst_bps, outfile=None):
    global objects
    data = defaultdict(list)
    for i, obj in enumerate(objects[imin:imax]):
        data['object_id'].append(obj.id)
        data['ra'].append(obj.ra)
        data['dec'].append(obj.dec)
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            sed = obj.get_total_observer_sed()
            for band in "gri":
                bp = lsst_bps[band]
                data[band].append(sed.calculateMagnitude(bp))
    df = pd.DataFrame(data)
    if outfile is None:
        outfile = f"refcat_mags_{imin:08d}_{imax:08d}.parquet"
    df.to_parquet(outfile)


def read_skyCatalogs(ra, dec, radius=0.5):
    region = skyCatalogs.Disk(ra, dec, radius*3600.0)
    mjd = 60390.3293054128
    obj_types = {'star'}

    skycatalog_yaml = ('/sdf/data/rubin/shared/ops-rehearsal-3/imSim_catalogs/'
                       'skyCatalogs/skyCatalog.yaml')
    skycatalog_root = os.path.dirname(skycatalog_yaml)

    sky_cat = skyCatalogs.open_catalog(skycatalog_yaml,
                                       skycatalog_root=skycatalog_root)
    obj_list = sky_cat.get_objects_by_region(region, mjd=mjd,
                                             obj_type_set=obj_types)
    return obj_list


#field = "COSMOS"
#ra, dec = 150.10, 2.18

#field = "DEEP_A0"
#ra, dec = 216., -12.50

#field = "DESI_SV3_R1"
#ra, dec = 179.6, 0.

#field = "Rubin_SV_095_-25"
#ra, dec = 95., -25.

#field = "Rubin_SV_125_-15"
#ra, dec = 125., -15.

#field = "Rubin_SV_225_-40"
#ra, dec = 225., -40.

#field = "Rubin_SV_250_2"
#ra, dec = 250., 2.

#field = "Rubin_SV_280_-48"
#ra, dec = 280., -48.

#field = "Rubin_SV_300_-41"
#ra, dec = 300., -41.

radius = 10.

initial_refcat_data = f"initial_refcat_mags_{field}_10deg.parquet"
if not os.path.isfile(initial_refcat_data):
    objects = read_skyCatalogs(ra, dec, radius=radius)
    nobj = len(objects)
    print("# skyCatalog objects:", nobj)

    processes = 32
    indexes = np.linspace(0, nobj+1, processes+1, dtype=int)
    with multiprocessing.Pool(processes=processes) as pool:
        workers = []
        for imin, imax in zip(indexes[:-1], indexes[1:]):
            args = (imin, imax)
            workers.append(pool.apply_async(compute_mags, args))
        pool.close()
        pool.join()
        _ = [worker.get() for worker in workers]

    files = sorted(glob.glob('refcat_mags*'))
    dfs = [pd.read_parquet(_) for _ in files]
    df = pd.concat(dfs)
    df.to_parquet(initial_refcat_data)
else:
    df = pd.read_parquet(initial_refcat_data)

# Simulate uncertainties and add error info.
# Baseline r-mag cut to control range over which spline fits are applied.
r_max = 23.
df0 = pd.DataFrame(df.query(f"r < {r_max}"))

mag_errors = MagErrors()
radec_errors = RADecErrors()
r_mags = df0['r'].to_numpy()
ra_err, dec_err = radec_errors(r_mags)
df0['ra'] += np.random.normal(loc=0, scale=ra_err)
df0['ra_err'] = ra_err*3600*1000.  # Convert to milliarcseconds
df0['dec'] += np.random.normal(loc=0, scale=dec_err)
df0['dec_err'] = dec_err*3600*1000.  # Convert to milliarcseconds

df0['r_err'] = mag_errors(r_mags, 'r')
df0['r'] += np.random.normal(loc=0, scale=df0['r_err'])

df0['g_err'] = mag_errors(df0['g'].to_numpy(), 'g')
df0['g'] += np.random.normal(loc=0, scale=df0['g_err'])

df0['i_err'] = mag_errors(df0['i'].to_numpy(), 'i')
df0['i'] += np.random.normal(loc=0, scale=df0['i_err'])


# Write csv file with refcat entries after final magnitude cut as been
# applied.
r_max_final = 21.
df = df0.query(f"r < {r_max_final}")

output_file = f"uw_stars_{field}_refmags_10deg.csv"
with open(output_file, 'w') as fobj:
    catalog_line = "object_id,ra,dec,ra_err,dec_err"
    for band in "gri":
        catalog_line += f",lsst_{band},lsst_{band}_mag_err"
    fobj.write(catalog_line + "\n")

    for _, row in df.iterrows():
        catalog_line = (f"{row.object_id},{row.ra},{row.dec},"
                        f"{row.ra_err},{row.dec_err}")
        for band in "gri":
            band_err = band + '_err'
            catalog_line += f",{row[band]},{row[band_err]}"
        fobj.write(catalog_line + "\n")
