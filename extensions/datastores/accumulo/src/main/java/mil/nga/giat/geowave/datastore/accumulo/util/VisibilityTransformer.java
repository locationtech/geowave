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
package mil.nga.giat.geowave.datastore.accumulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.StringUtils;

public class VisibilityTransformer implements
		Transformer
{
	private String transformingRegex;
	private String replacement;

	public VisibilityTransformer(
			final String transformingRegex,
			final String replacement ) {
		super();
		this.transformingRegex = transformingRegex;
		this.replacement = replacement;
	}

	@Override
	public Pair<Key, Value> transform(
			Pair<Key, Value> entry ) {
		Key originalKey = entry.getKey();
		Text visibiltity = originalKey.getColumnVisibility();
		String newVisibility = visibiltity.toString().replaceFirst(
				transformingRegex,
				replacement);
		if (newVisibility.length() > 0) {
			char one = newVisibility.charAt(0);
			// strip off any ending options
			if (one == '&' || one == '|') newVisibility = newVisibility.substring(1);
		}
		byte[] row = originalKey.getRowData().toArray();
		byte[] cf = originalKey.getColumnFamilyData().toArray();
		byte[] cq = originalKey.getColumnQualifierData().toArray();
		long timestamp = originalKey.getTimestamp();
		byte[] cv = newVisibility.getBytes(StringUtils.GEOWAVE_CHAR_SET);
		Key newKey = new Key(
				row,
				0,
				row.length,
				cf,
				0,
				cf.length,
				cq,
				0,
				cq.length,
				cv,
				0,
				cv.length,
				timestamp + 1);

		return Pair.of(
				newKey,
				entry.getValue());
	}

}
