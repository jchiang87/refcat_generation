from lsst.meas.algorithms.convertRefcatManager import ConvertRefcatManager


__all__ = ['ConvertOr3RefcatManager']


class ConvertOr3RefcatManager(ConvertRefcatManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _setCoordinateCovariance(self, record, row):
        """Coordinate covariance will not be used, so set to zero.
        """
        outputParams = ['coord_ra', 'coord_dec', 'pm_ra', 'pm_dec', 'parallax']
        for i in range(5):
            for j in range(i):
                record.set(self.key_map[f'{outputParams[j]}_{outputParams[i]}_Cov'], 0)
