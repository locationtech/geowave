/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.index.text;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class TextExactMatchFilter implements
		QueryFilter
{

	private String fieldName;
	private String matchValue;
	private boolean caseSensitive;

	public TextExactMatchFilter() {
		super();
	}

	public TextExactMatchFilter(
			final String fieldName,
			final String matchValue,
			final boolean caseSensitive ) {
		super();
		this.fieldName = fieldName;
		this.matchValue = matchValue;
		this.caseSensitive = caseSensitive;
	}

	public String getFieldName() {
		return fieldName;
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
		final ByteArray stringBytes = (ByteArray) persistenceEncoding.getCommonData().getValue(
				fieldName);
		if (stringBytes != null) {
			final String value = stringBytes.getString();
			return caseSensitive ? matchValue.equals(value) : matchValue.equalsIgnoreCase(value);
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
		final byte[] matchValueBytes = StringUtils.stringToBinary(matchValue);
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldNameBytes.length + 4 + matchValueBytes.length + 4);
		bb.putInt(fieldNameBytes.length);
		bb.put(fieldNameBytes);
		bb.putInt(matchValueBytes.length);
		bb.put(matchValueBytes);
		bb.putInt(caseSensitive ? 1 : 0);
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldNameBytes = new byte[bb.getInt()];
		bb.get(fieldNameBytes);
		fieldName = StringUtils.stringFromBinary(fieldNameBytes);
		final byte[] matchValueBytes = new byte[bb.getInt()];
		bb.get(matchValueBytes);
		matchValue = StringUtils.stringFromBinary(matchValueBytes);
		caseSensitive = (bb.getInt() == 1) ? true : false;
	}
}
