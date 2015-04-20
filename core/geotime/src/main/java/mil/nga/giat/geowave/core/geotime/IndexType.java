package mil.nga.giat.geowave.core.geotime;

import mil.nga.giat.geowave.core.geotime.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;

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
		return dimensionalityType.name() + "_" + dataType.name() + "_IDX";
	}

	public Index createDefaultIndex() {
		return new CustomIdIndex(
				createDefaultIndexStrategy(),
				getDefaultIndexModel(),
				new ByteArrayId(
						getDefaultId()));
	}

	// This is a support class to assist in creating default indices as enums
	// aren't handled well in Jace/JNI
	public static class JaceIndexType
	{
		public static Index createSpatialVectorIndex() {
			return IndexType.SPATIAL_VECTOR.createDefaultIndex();
		}

		public static Index createSpatialTemporalVectorIndex() {
			return IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex();
		}

		public static Index createSpatialRasterIndex() {
			return IndexType.SPATIAL_RASTER.createDefaultIndex();
		}

		public static Index createSpatialTemporalRasterIndex() {
			return IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex();
		}
	}
}
