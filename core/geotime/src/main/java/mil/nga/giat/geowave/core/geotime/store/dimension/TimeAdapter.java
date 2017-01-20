package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.store.dimension.Time.TimeRange;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.Timestamp;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * This adapter can be used for reading and writing Time fields within GeoWave
 * and enables a temporal field definition.
 *
 */
public class TimeAdapter implements
		FieldReader<Time>,
		FieldWriter<Object, Time>
{
	public TimeAdapter() {}

	@Override
	public byte[] writeField(
			final Time time ) {
		return time.toBinary();
	}

	@Override
	public Time readField(
			final byte[] bytes ) {
		Time retVal;
		// this is less generic than using reflection but essential for
		// performance
		if (bytes.length > 8) {
			// it must be a time range
			retVal = new TimeRange();
		}
		else {
			// it must be a timestamp
			retVal = new Timestamp();
		}
		retVal.fromBinary(bytes);
		return retVal;
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final ByteArrayId fieldId,
			final Time time ) {
		return time.getVisibility();
	}
}
