package org.locationtech.geowave.core.store.adapter.statistics;

public class StatisticsId
{
	private StatisticsType<?, ?> type;
	private String extendedId;

	public StatisticsId(
			StatisticsType<?, ?> type,
			String extendedId ) {
		super();
		this.type = type;
		this.extendedId = extendedId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((extendedId == null) ? 0 : extendedId.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		StatisticsId other = (StatisticsId) obj;
		if (extendedId == null) {
			if (other.extendedId != null) return false;
		}
		else if (!extendedId.equals(other.extendedId)) return false;
		if (type == null) {
			if (other.type != null) return false;
		}
		else if (!type.equals(other.type)) return false;
		return true;
	}

	public StatisticsType<?, ?> getType() {
		return type;
	}

	public String getExtendedId() {
		return extendedId;
	}
}
