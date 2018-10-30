package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.geotime.store.dimension.Time.Timestamp;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class TimeReader implements
		FieldReader<Time>
{
	public TimeReader() {}

	@Override
	public Time readField(
			final byte[] bytes ) {
		Time retVal;
		// this is less generic than using the persistable interface but is a
		// little better for performance
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
}
