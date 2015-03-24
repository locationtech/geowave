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
			buffer.append(time.getLocalName());
		}
		else
			buffer.append("-");
		if ((startRange != null) && (endRange != null)) {
			buffer.append(
					':').append(
					startRange.getLocalName()).append(
					':').append(
					endRange.getLocalName());
		}
		else {
			buffer.append("::");
		}
		return StringUtils.stringToBinary(buffer.toString());
	}

	public void fromBinary(
			final SimpleFeatureType type,
			final byte[] image ) {
		final String buf = StringUtils.stringFromBinary(image);
		String[] splits = buf.split(":");

		if (!"-".equals(splits[0])) {
			time = type.getDescriptor(splits[0].trim());
		}
		if (splits.length > 1 && splits[1].length() > 0) {
			startRange = type.getDescriptor(splits[1]);
			endRange = type.getDescriptor(splits[2]);

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
		if (this.getStartRange() != null) {
			if (this.getEndRange() != null) {
				this.setTime(null);
			}
			else {
				if (getTime() == null) {
					this.setTime(getStartRange());
				}
				this.setStartRange(null);
			}
		}
		else if (this.getEndRange() != null && this.getStartRange() == null) {
			if (getTime() == null) {
				this.setTime(getEndRange());
			}
			this.setEndRange(null);
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endRange == null) ? 0 : endRange.hashCode());
		result = prime * result + ((startRange == null) ? 0 : startRange.hashCode());
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TimeDescriptors other = (TimeDescriptors) obj;
		if (endRange == null) {
			if (other.endRange != null) return false;
		}
		else if (!endRange.equals(other.endRange)) return false;
		if (startRange == null) {
			if (other.startRange != null) return false;
		}
		else if (!startRange.equals(other.startRange)) return false;
		if (time == null) {
			if (other.time != null) return false;
		}
		else if (!time.equals(other.time)) return false;
		return true;
	}

}
