package mil.nga.giat.geowave.store.index;

import mil.nga.giat.geowave.index.NumericIndexStrategyFactory;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.SpatialFactory;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.SpatialTemporalFactory;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.dimension.LatitudeField;
import mil.nga.giat.geowave.store.dimension.LongitudeField;
import mil.nga.giat.geowave.store.dimension.TimeField;

public enum DimensionalityType {
	SPATIAL(
			new BasicIndexModel(
					new DimensionField[] {
						new LongitudeField(),
						new LatitudeField()
					}),
			new SpatialFactory()),
	SPATIAL_TEMPORAL(
			new BasicIndexModel(
					new DimensionField[] {
						new LongitudeField(),
						new LatitudeField(),
						new TimeField(
								Unit.YEAR)
					}),
			new SpatialTemporalFactory()),
	OTHER(
			null,
			null);
	private final CommonIndexModel defaultIndexModel;
	private final NumericIndexStrategyFactory indexStrategyFactory;

	private DimensionalityType(
			final CommonIndexModel defaultIndexModel,
			final NumericIndexStrategyFactory indexStrategyFactory ) {
		this.defaultIndexModel = defaultIndexModel;
		this.indexStrategyFactory = indexStrategyFactory;
	}

	public CommonIndexModel getDefaultIndexModel() {
		return defaultIndexModel;
	}

	public NumericIndexStrategyFactory getIndexStrategyFactory() {
		return indexStrategyFactory;
	}
}
