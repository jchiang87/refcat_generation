config.dataset_config.ref_dataset_name = "uw_stars_20240130"

config.n_processes = 32

config.id_name = "object_id"
config.ra_name = "ra"
config.dec_name = "dec"
config.ra_err_name = "ra_err"
config.dec_err_name = "dec_err"
config.coord_err_unit = "milliarcsecond"
config.mag_column_list = [f"lsst_{band}" for band in 'gri']
config.mag_err_column_map = {f"lsst_{band}": f"lsst_{band}_mag_err" for band in 'gri'}
