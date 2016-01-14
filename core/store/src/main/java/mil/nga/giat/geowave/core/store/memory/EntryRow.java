package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

public class EntryRow implements
		Comparable<EntryRow>
{
	final EntryRowID rowId;
	final DataStoreEntryInfo info;
	final Object entry;

	public EntryRow(
			final ByteArrayId rowId,
			final Object entry,
			final DataStoreEntryInfo info ) {
		super();
		this.rowId = new EntryRowID(
				rowId.getBytes());
		this.entry = entry;
		this.info = info;
	}

	public EntryRowID getTableRowId() {
		return rowId;
	}

	public ByteArrayId getRowId() {
		return new ByteArrayId(
				rowId.getRowId());
	}

	public List<FieldInfo<?>> getColumns() {
		return info.getFieldInfo();
	}

	@Override
	public int compareTo(
			EntryRow o ) {
		return rowId.compareTo(((EntryRow) o).rowId);
	}

	public Object getEntry() {
		return entry;
	}

	public DataStoreEntryInfo getInfo() {
		return info;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entry == null) ? 0 : entry.hashCode());
		result = prime * result + ((rowId == null) ? 0 : rowId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		EntryRow other = (EntryRow) obj;
		if (entry == null) {
			if (other.entry != null) return false;
		}
		else if (!entry.equals(other.entry)) return false;
		if (rowId == null) {
			if (other.rowId != null) return false;
		}
		else if (!rowId.equals(other.rowId)) return false;
		return true;
	}

}
