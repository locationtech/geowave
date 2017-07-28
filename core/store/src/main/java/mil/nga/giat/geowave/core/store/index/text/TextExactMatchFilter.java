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
package mil.nga.giat.geowave.core.store.index.text;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class TextExactMatchFilter implements
		DistributableQueryFilter
{

	private ByteArrayId fieldId;
	private String matchValue;
	private boolean caseSensitive;

	public TextExactMatchFilter() {
		super();
	}

	public TextExactMatchFilter(
			final ByteArrayId fieldId,
			final String matchValue,
			final boolean caseSensitive ) {
		super();
		this.fieldId = fieldId;
		this.matchValue = matchValue;
		this.caseSensitive = caseSensitive;
	}

	public ByteArrayId getFieldId() {
		return fieldId;
	}

	public String getMatchValue() {
		return matchValue;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId stringBytes = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (stringBytes != null) {
			String value = stringBytes.getString();
			return caseSensitive ? matchValue.equals(value) : matchValue.equalsIgnoreCase(value);
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] fieldIdBytes = fieldId.getBytes();
		final byte[] matchValueBytes = StringUtils.stringToBinary(matchValue);
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldIdBytes.length + 4 + matchValueBytes.length + 4);
		bb.putInt(fieldIdBytes.length);
		bb.put(fieldIdBytes);
		bb.putInt(matchValueBytes.length);
		bb.put(matchValueBytes);
		bb.putInt(caseSensitive ? 1 : 0);
		return bb.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldIdBytes = new byte[bb.getInt()];
		bb.get(fieldIdBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		final byte[] matchValueBytes = new byte[bb.getInt()];
		bb.get(matchValueBytes);
		matchValue = StringUtils.stringFromBinary(matchValueBytes);
		caseSensitive = (bb.getInt() == 1) ? true : false;
	}
}
