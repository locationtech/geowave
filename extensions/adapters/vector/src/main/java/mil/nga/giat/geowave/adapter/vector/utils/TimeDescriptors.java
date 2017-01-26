package mil.nga.giat.geowave.adapter.vector.utils;

import mil.nga.giat.geowave.core.geotime.TimeUtils;
import mil.nga.giat.geowave.core.index.StringUtils;

import java.util.Locale;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * 
 * Describes temporally indexed attributes associated with a feature type.
 * 
 */
public class TimeDescriptors
{
	private AttributeDescriptor startRange;
	private AttributeDescriptor endRange;
	private AttributeDescriptor time;

	public TimeDescriptors() {
		super();
		time = null;
		startRange = null;
		endRange = null;
	}

	public TimeDescriptors(
			final AttributeDescriptor time ) {
		super();
		this.time = time;
		startRange = null;
		endRange = null;
	}

	public TimeDescriptors(
			final SimpleFeatureType type,
			final TimeDescriptorConfiguration configuration ) {
		this.update(
				type,
				configuration);
	}

	public TimeDescriptors(
			final AttributeDescriptor startRange,
			final AttributeDescriptor endRange ) {
		super();
		time = null;
		this.startRange = startRange;
		this.endRange = endRange;
	}

	public void update(
			final SimpleFeatureType type,
			final TimeDescriptorConfiguration configuration ) {
		if (configuration.timeName != null) {
			time = type.getDescriptor(configuration.timeName);
		}
		if (configuration.startRangeName != null) {
			startRange = type.getDescriptor(configuration.startRangeName);
		}
		if (configuration.endRangeName != null) {
			endRange = type.getDescriptor(configuration.endRangeName);
		}
	}

	public void setStartRange(
			final AttributeDescriptor startRange ) {
		this.startRange = startRange;
	}

	public void setEndRange(
			final AttributeDescriptor endRange ) {
		this.endRange = endRange;
	}

	public void setTime(
			final AttributeDescriptor time ) {
		this.time = time;
	}

	public AttributeDescriptor getStartRange() {
		return startRange;
	}

	public AttributeDescriptor getEndRange() {
		return endRange;
	}

	public AttributeDescriptor getTime() {
		return time;
	}

	public boolean hasTime() {
		return (time != null) || ((startRange != null) && (endRange != null));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((endRange == null) ? 0 : endRange.hashCode());
		result = (prime * result) + ((startRange == null) ? 0 : startRange.hashCode());
		result = (prime * result) + ((time == null) ? 0 : time.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final TimeDescriptors other = (TimeDescriptors) obj;
		if (endRange == null) {
			if (other.endRange != null) {
				return false;
			}
		}
		else if (!endRange.equals(other.endRange)) {
			return false;
		}
		if (startRange == null) {
			if (other.startRange != null) {
				return false;
			}
		}
		else if (!startRange.equals(other.startRange)) {
			return false;
		}
		if (time == null) {
			if (other.time != null) {
				return false;
			}
		}
		else if (!time.equals(other.time)) {
			return false;
		}
		return true;
	}

	public static class TimeDescriptorConfiguration implements
			SimpleFeatureUserDataConfiguration
	{

		private static final long serialVersionUID = -664252700036603897L;
		private String startRangeName = null;
		private String endRangeName = null;
		private String timeName = null;

		public TimeDescriptorConfiguration() {

		}

		public TimeDescriptorConfiguration(
				final SimpleFeatureType type ) {
			this.configureFromType(type);
		}

		public String getStartRangeName() {
			return startRangeName;
		}

		public void setStartRangeName(
				final String startRangeName ) {
			this.startRangeName = startRangeName;
		}

		public String getEndRangeName() {
			return endRangeName;
		}

		public void setEndRangeName(
				final String endRangeName ) {
			this.endRangeName = endRangeName;
		}

		public String getTimeName() {
			return timeName;
		}

		public void setTimeName(
				final String timeName ) {
			this.timeName = timeName;
		}

		@Override
		public void updateType(
				final SimpleFeatureType persistType ) {
			for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
				final Class<?> bindingClass = attrDesc.getType().getBinding();
				if (TimeUtils.isTemporal(bindingClass)) {
					attrDesc.getUserData().put(
							"time",
							Boolean.FALSE);
				}
			}
			if (startRangeName != null) {
				persistType.getDescriptor(
						startRangeName).getUserData().put(
						"start",
						Boolean.TRUE);
			}
			if (endRangeName != null) {
				persistType.getDescriptor(
						endRangeName).getUserData().put(
						"end",
						Boolean.TRUE);
			}
			if (timeName != null) {
				persistType.getDescriptor(
						timeName).getUserData().put(
						"time",
						Boolean.TRUE);
			}
		}

		@Override
		public void configureFromType(
				final SimpleFeatureType persistType ) {
			for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
				final Class<?> bindingClass = attrDesc.getType().getBinding();
				if (TimeUtils.isTemporal(bindingClass)) {
					final Boolean isTime = (Boolean) attrDesc.getUserData().get(
							"time");
					if (isTime != null) {
						if (isTime.booleanValue()) {
							setTimeName(attrDesc.getLocalName());
							setStartRangeName(null);
							setEndRangeName(null);
							break;
						}
					}
					final Boolean isStart = (Boolean) attrDesc.getUserData().get(
							"start");
					final Boolean isEnd = (Boolean) attrDesc.getUserData().get(
							"end");
					if ((isStart != null) && isStart.booleanValue()) {
						setStartRangeName(attrDesc.getLocalName());
					}
					else if ((isStart == null) && (getStartRangeName() == null) && attrDesc.getLocalName().toLowerCase(
							Locale.ENGLISH).startsWith(
							"start")) {
						setStartRangeName(attrDesc.getLocalName());
					}
					else if ((isEnd != null) && isEnd.booleanValue()) {
						setEndRangeName(attrDesc.getLocalName());
					}
					else if ((isEnd == null) && (getEndRangeName() == null) && attrDesc.getLocalName().toLowerCase(
							Locale.ENGLISH).startsWith(
							"end")) {
						setEndRangeName(attrDesc.getLocalName());
					}
					else if ((isTime == null) && (getTimeName() == null)) {
						setTimeName(attrDesc.getLocalName());
					}
				}
			}
			if (getStartRangeName() != null) {
				if (getEndRangeName() != null) {
					setTimeName(null);
				}
				else {
					if (getTimeName() == null) {
						setTimeName(getStartRangeName());
					}
					setStartRangeName(null);
				}
			}
			else if ((getEndRangeName() != null) && (getStartRangeName() == null)) {
				if (getTimeName() == null) {
					setTimeName(getEndRangeName());
				}
				setEndRangeName(null);
			}
		}

	}

}
