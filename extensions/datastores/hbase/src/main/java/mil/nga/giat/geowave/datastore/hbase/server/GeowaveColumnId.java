package mil.nga.giat.geowave.datastore.hbase.server;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface GeowaveColumnId
{

}

class ShortColumnId implements
		GeowaveColumnId
{
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + columnId;
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ShortColumnId other = (ShortColumnId) obj;
		if (columnId != other.columnId) return false;
		return true;
	}

	private short columnId;

	public ShortColumnId(
			short columnId ) {
		this.columnId = columnId;
	}
}

class ByteArrayColumnId implements
		GeowaveColumnId
{

	private ByteArrayId columnId;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnId == null) ? 0 : columnId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ByteArrayColumnId other = (ByteArrayColumnId) obj;
		if (columnId == null) {
			if (other.columnId != null) return false;
		}
		else if (!columnId.equals(other.columnId)) return false;
		return true;
	}

	public ByteArrayColumnId(
			ByteArrayId columnId ) {
		this.columnId = columnId;
	}
}