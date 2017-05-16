/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.EntryRowID;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

public class MemoryEntryRow implements
		Comparable<MemoryEntryRow>
{
	final EntryRowID rowId;
	final DataStoreEntryInfo info;
	final Object entry;

	public MemoryEntryRow(
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
			MemoryEntryRow o ) {
		return rowId.compareTo(((MemoryEntryRow) o).rowId);
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
		MemoryEntryRow other = (MemoryEntryRow) obj;
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
