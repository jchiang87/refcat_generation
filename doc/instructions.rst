Steps for creating reference catalogs from skyCatalogs input
------------------------------------------------------------
Reference: `Refcat instructions at pipelines.lsst.io <https://pipelines.lsst.io/modules/lsst.meas.algorithms/creating-a-reference-catalog.html#how-to-generate-an-lsst-reference-catalog>`_

1. Set up the skyCatalogs directory.

2. Edit ``scripts/make_refcat_input.py`` to cover the desired fields.

3. Run ``scripts/make_refcat_input.py`` (using multiprocessing) to

   * generate parquet files with magnitudes in each band.
   * compute position and magnitude errors and write the csv files.

4. Edit ``config/<my_catalog>_config.py`` to set the dataset name, number
   of processes to uese, etc..

5. Run the Rubin converstion script:

   .. code-block:: bash

      $ convertReferenceCatalog . <my_catalog>_config.py "*.csv"

   This will generate FITS files in the ``.ref_dataset_name``
   subfolder and a ``filename_to_htm.ecsv`` file for ingesting into
   the butler repo.  By convention, ``<my_catalog>`` is set to a
   descriptive name for the catalog combined with the date it was
   generated, e.g., ```euclid_stars_20260315``

6. Register these refcats as a dataset type and ingest into the butler
   repository.  This must be done from the directory where the refcats
   were generated since relative paths are written to
   ``filename_to_htm.ecsv``:

   .. code-block:: bash

   $ cd <directory containing filename_to_htm.escv>
   $ butler register-dataset-type ${REPO} <my_catalog> SimpleCatalog htm7
   $ butler ingest-files -t direct ${REPO} <my_catalog>  refcats/<my_catalog> filename_to_htm.ecsv
   $ butler collection-chain ./repo --mode extend refcats refcats/<my_catalog>

