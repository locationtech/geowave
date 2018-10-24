package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.threeten.extra.Interval;

public class CommonIndexTimeRangeAggregation<P extends Persistable> extends
		TimeRangeAggregation<P, CommonIndexedPersistenceEncoding> implements
		CommonIndexAggregation<P, Interval>
{

	@Override
	protected Interval getInterval(
			final CommonIndexedPersistenceEncoding entry ) {
		return null;
	}

}
