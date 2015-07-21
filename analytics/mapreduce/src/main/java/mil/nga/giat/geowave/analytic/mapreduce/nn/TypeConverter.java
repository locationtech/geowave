package mil.nga.giat.geowave.analytic.mapreduce.nn;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * Convert object consumed by NN to a 'smaller' object pertinent to any subclass
 * algorithms
 * 
 * @param <TYPE>
 */
public interface TypeConverter<TYPE>
{
	public TYPE convert(
			ByteArrayId id,
			Object o );
}
