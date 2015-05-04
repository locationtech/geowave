package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

/**
 * A common index value can be very generic but must have a way to identify its
 * visibility
 * 
 */
public interface CommonIndexValue
{
	public byte[] getVisibility();

	public void setVisibility(
			byte[] visibility );

	public boolean overlaps(
			DimensionField[] field,
			NumericData[] rangeData );
}
