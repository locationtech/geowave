package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

public class VectorTimeRangeAggregation extends
		TimeRangeAggregation<FieldNameParam, SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(VectorTimeRangeAggregation.class);
	private FieldNameParam fieldNameParam;
	private final Map<String, TimeDescriptors> descMap = new HashMap<>();

	public VectorTimeRangeAggregation() {
		this(
				null);
	}

	public VectorTimeRangeAggregation(
			final FieldNameParam fieldNameParam ) {
		super();
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	public FieldNameParam getParameters() {
		return fieldNameParam;
	}

	@Override
	public void setParameters(
			final FieldNameParam fieldNameParam ) {
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	protected Interval getInterval(
			final SimpleFeature entry ) {
		if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
			return TimeUtils.getInterval(
					entry,
					fieldNameParam.getFieldName());
		}
		final String type = entry.getType().getName().getLocalPart();
		TimeDescriptors desc = descMap.get(type);
		if (desc == null) {
			desc = TimeUtils.inferTimeAttributeDescriptor(entry.getFeatureType());
			descMap.put(
					type,
					desc);
		}
		if ((desc.getStartRange() != null) && (desc.getEndRange() != null)) {
			final Object start = entry.getAttribute(desc.getStartRange().getName());
			final Object end = entry.getAttribute(desc.getStartRange().getName());
			if ((start == null) || (end == null)) {
				LOGGER.warn("start or end value is null, ignoring feature");
				return null;
			}
			// TODO we may want to sanity check that start is less than end?
			return Interval.of(
					Instant.ofEpochMilli(TimeUtils.getTimeMillis(start)),
					Instant.ofEpochMilli(TimeUtils.getTimeMillis(end)));
		}

		else if (desc.getTime() != null) {
			final Object time = entry.getAttribute(desc.getTime().getName());
			if ((time == null)) {
				LOGGER.warn("time attribute value is null, ignoring feature");
				return null;
			}
			final Instant instant = Instant.ofEpochMilli(TimeUtils.getTimeMillis(time));
			return Interval.of(
					instant,
					instant);
		}
		LOGGER.error("time field not found for type '" + entry.getFeatureType().getTypeName()
				+ "'. Consider explicitly setting field name.");
		return null;

	}
}
