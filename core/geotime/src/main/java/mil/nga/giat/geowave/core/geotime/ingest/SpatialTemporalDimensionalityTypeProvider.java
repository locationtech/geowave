package mil.nga.giat.geowave.core.geotime.ingest;

import mil.nga.giat.geowave.core.geotime.DimensionalityType;
import mil.nga.giat.geowave.core.ingest.IndexCompatibilityVisitor;
import mil.nga.giat.geowave.core.ingest.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.index.Index;

public class SpatialTemporalDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{
	@Override
	public IndexCompatibilityVisitor getCompatibilityVisitor() {
		return new SpatialTemporalIndexCompatibilityVisitor();
	}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial-temporal";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Latitude, Longitude, and Time definitions.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial so that the default
		// will be spatial over spatial-temporal
		return 5;
	}

	private static class SpatialTemporalIndexCompatibilityVisitor implements
			IndexCompatibilityVisitor
	{

		@Override
		public boolean isCompatible(
				final Index index ) {
			return DimensionalityType.SPATIAL_TEMPORAL.isCompatible(index);
		}

	}
}
