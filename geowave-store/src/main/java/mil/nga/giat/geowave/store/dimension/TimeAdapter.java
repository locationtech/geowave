package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;

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
		return PersistenceUtils.toBinary(time);
	}

	@Override
	public Time readField(
			final byte[] bytes ) {
		return PersistenceUtils.fromBinary(
				bytes,
				Time.class);
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final ByteArrayId fieldId,
			final Time time ) {
		return time.getVisibility();
	}
}
