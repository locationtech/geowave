package mil.nga.giat.geowave.store.index;

import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.NumericIndexStrategy.SpatialFactory;
import mil.nga.giat.geowave.index.NumericIndexStrategy.SpatialTemporalFactory;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.dimension.LatitudeField;
import mil.nga.giat.geowave.store.dimension.LongitudeField;
import mil.nga.giat.geowave.store.dimension.TimeField;

/**
 * This is an enumeration of default commonly used Indices supported (with
 * generally reasonable default configuration). Any other index can be
 * instantiated and used outside of this enumerated list. This is merely
 * provided for convenience.
 * 
 */
public enum IndexType {
	SPATIAL(
			// TODO: enforce the index strategy dimensions are in sync with the
			// index model dimensions
			SpatialFactory.createIndexStrategy(),
			new BasicIndexModel(
					new DimensionField[] {
						new LongitudeField(),
						new LatitudeField()
					})),
	SPATIAL_TEMPORAL(
			// TODO: enforce the index strategy dimensions are in sync with the
			// index model dimensions
			SpatialTemporalFactory.createIndexStrategy(),
			new BasicIndexModel(
					new DimensionField[] {
						new LongitudeField(),
						new LatitudeField(),
						new TimeField(
								Unit.YEAR)
					}));

	private final NumericIndexStrategy defaultStrategy;
	private final CommonIndexModel defaultIndexModel;

	private IndexType(
			final NumericIndexStrategy defaultStrategy,
			final CommonIndexModel defaultIndexModel ) {
		this.defaultStrategy = defaultStrategy;
		this.defaultIndexModel = defaultIndexModel;
	}

	public NumericIndexStrategy getDefaultIndexStrategy() {
		return defaultStrategy;
	}

	public CommonIndexModel getDefaultIndexModel() {
		return defaultIndexModel;
	}

	public Index createDefaultIndex() {
		return new Index(
				defaultStrategy,
				defaultIndexModel);
	}
}
