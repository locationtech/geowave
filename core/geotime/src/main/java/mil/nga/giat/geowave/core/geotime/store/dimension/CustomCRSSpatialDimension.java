package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;

public interface CustomCRSSpatialDimension extends
		NumericDimensionDefinition
{
	public byte getAxis();
}
