package mil.nga.giat.geowave.core.store.filter;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

public interface FilterFactory
{
	public DistributableQueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields );
}
