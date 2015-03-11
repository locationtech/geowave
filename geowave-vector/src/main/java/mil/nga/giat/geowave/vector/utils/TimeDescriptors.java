package mil.nga.giat.geowave.vector.utils;

import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.TimeUtils;

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
			final AttributeDescriptor startRange,
			final AttributeDescriptor endRange ) {
		super();
		time = null;
		this.startRange = startRange;
		this.endRange = endRange;
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
		return time != null || (startRange != null && endRange != null);
	}
	
	public byte[] toBinary() {
		final StringBuffer buffer = new StringBuffer();
		if (time != null) {
			buffer.append(
					time.getLocalName()).append(
					"::");
		}
		else if ((startRange != null) && (endRange != null)) {
			buffer.append(
					':').append(
					startRange.getLocalName()).append(
					':').append(
					endRange.getLocalName());
		}
		return StringUtils.stringToBinary(buffer.toString());
	}

	public void fromBinary(
			final SimpleFeatureType type,
			final byte[] image ) {
		final String buf = StringUtils.stringFromBinary(image);
		if (buf.startsWith(":")) {
			// range
			startRange = type.getDescriptor(buf.substring(
					1,
					buf.indexOf(
							':',
							1)));
			endRange = type.getDescriptor(buf.substring(buf.lastIndexOf(':') + 1));

		}
		else if (buf.length() > 0) {
			time = type.getDescriptor(buf.substring(
					0,
					buf.indexOf(':')));
		}
	}

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
		if (startRange != null) {
			persistType.getDescriptor(
					startRange.getLocalName()).getUserData().put(
					"start",
					Boolean.TRUE);
		}
		if (endRange != null) {
			persistType.getDescriptor(
					endRange.getLocalName()).getUserData().put(
					"end",
					Boolean.TRUE);
		}
		if (time != null) {
			persistType.getDescriptor(
					time.getLocalName()).getUserData().put(
					"time",
					Boolean.TRUE);
		}
	}

	public void inferType(
			final SimpleFeatureType persistType ) {
		for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
			final Class<?> bindingClass = attrDesc.getType().getBinding();
			if (TimeUtils.isTemporal(bindingClass)) {
				final Boolean isTime = (Boolean) attrDesc.getUserData().get(
						"time");
				if (isTime != null) {
					if (isTime.booleanValue()) {
						setTime(attrDesc);
						// override
						setStartRange(null);
						setEndRange(null);
						break;
					}
				}
				final Boolean isStart = (Boolean) attrDesc.getUserData().get(
						"start");
				final Boolean isEnd = (Boolean) attrDesc.getUserData().get(
						"end");
				if ((isStart != null) && isStart.booleanValue()) {
					setStartRange(attrDesc);
				}
				else if ((isStart == null) && (getStartRange() == null) && attrDesc.getLocalName().toLowerCase().startsWith(
						"start")) {
					setStartRange(attrDesc);
				}
				else if ((isEnd != null) && isEnd.booleanValue()) {
					setEndRange(attrDesc);
				}
				else if ((isEnd == null) && (getEndRange() == null) && attrDesc.getLocalName().toLowerCase().startsWith(
						"end")) {
					setEndRange(attrDesc);
				}
				else if (isTime == null && getTime() == null) {
					setTime(attrDesc);
				}
			}
		}
	}
}
