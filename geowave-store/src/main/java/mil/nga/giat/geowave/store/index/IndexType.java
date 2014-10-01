package mil.nga.giat.geowave.store.index;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;

/**
 * This is an enumeration of default commonly used Indices supported (with
 * generally reasonable default configuration). Any other index can be
 * instantiated and used outside of this enumerated list. This is merely
 * provided for convenience.
 *
 */
public enum IndexType {
	SPATIAL_VECTOR(
			DimensionalityType.SPATIAL,
			DataType.VECTOR),
	SPATIAL_RASTER(
			DimensionalityType.SPATIAL,
			DataType.RASTER),
	SPATIAL_TEMPORAL_VECTOR(
			DimensionalityType.SPATIAL_TEMPORAL,
			DataType.VECTOR),
	SPATIAL_TEMPORAL_RASTER(
			DimensionalityType.SPATIAL_TEMPORAL,
			DataType.RASTER);

	private DimensionalityType dimensionalityType;
	private DataType dataType;

	private IndexType(
			final DimensionalityType dimensionalityType,
			final DataType dataType ) {
		this.dimensionalityType = dimensionalityType;
		this.dataType = dataType;
	}

	public NumericIndexStrategy createDefaultIndexStrategy() {
		return dimensionalityType.getIndexStrategyFactory().createIndexStrategy(
				dataType);
	}

	public CommonIndexModel getDefaultIndexModel() {
		return dimensionalityType.getDefaultIndexModel();
	}

	public String getDefaultId() {
		return dimensionalityType.name() + "_" + dataType.name() + "_INDEX";
	}

	public Index createDefaultIndex() {
		return new CustomIdIndex(
				createDefaultIndexStrategy(),
				getDefaultIndexModel(),
				dimensionalityType,
				dataType,
				new ByteArrayId(
						getDefaultId()));
	}
}
