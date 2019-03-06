from .config import config
from .base_models import Index

class SpatialIndex(Index):
    """Geotools SpatialIndex."""
    def __init__(self):
        j_spatial_idx_builder = config.MODULE__geotime_ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder()
        j_idx = j_spatial_idx_builder.createIndex()
        super().__init__(config.GATEWAY, j_idx)

class SpatialTemporalIndex(Index):
     """Geotools SpatialTemporalIndex"""
     def __init__(self):
        j_spat_temp_idx_builder = config.MODULE__geotime_ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder()
        j_idx = j_spat_temp_idx_builder.createIndex()
        super().__init__(config.GATEWAY, j_idx)