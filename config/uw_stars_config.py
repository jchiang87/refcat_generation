config.dataset_config.ref_dataset_name = "uw_stars"

config.n_processes = 1

config.id_name = "object_id"
config.ra_name = "ra"
config.dec_name = "dec"
config.mag_column_list = [f"lsst_{band}_mag" for band in 'ugrizy']

