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
package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class AttributeSubsettingIterator extends
		TransformingIterator
{
	private static final int ITERATOR_PRIORITY = QueryFilterIterator.QUERY_ITERATOR_PRIORITY + 1;
	private static final String ITERATOR_NAME = "ATTRIBUTE_SUBSETTING_ITERATOR";

	private static final String FIELD_SUBSET_BITMASK = "fieldsBitmask";
	public static final String WHOLE_ROW_ENCODED_KEY = "wholerow";
	private byte[] fieldSubsetBitmask;
	private boolean wholeRowEncoded;

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		while (input.hasTop()) {
			final Key wholeRowKey = input.getTopKey();
			final Value wholeRowVal = input.getTopValue();
			final SortedMap<Key, Value> rowMapping;
			if (wholeRowEncoded) {
				rowMapping = WholeRowIterator.decodeRow(
						wholeRowKey,
						wholeRowVal);
			}
			else {
				rowMapping = new TreeMap<Key, Value>();
				rowMapping.put(
						wholeRowKey,
						wholeRowVal);
			}
			final List<Key> keyList = new ArrayList<>();
			final List<Value> valList = new ArrayList<>();
			Text adapterId = null;

			for (final Entry<Key, Value> row : rowMapping.entrySet()) {
				final Key currKey = row.getKey();
				final Value currVal = row.getValue();
				if (adapterId == null) {
					adapterId = currKey.getColumnFamily();
				}
				final byte[] originalBitmask = currKey.getColumnQualifierData().getBackingArray();
				final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
						originalBitmask,
						fieldSubsetBitmask);
				if (BitmaskUtils.isAnyBitSet(newBitmask)) {
					if (!Arrays.equals(
							newBitmask,
							originalBitmask)) {
						keyList.add(replaceColumnQualifier(
								currKey,
								new Text(
										newBitmask)));
						valList.add(constructNewValue(
								currVal,
								originalBitmask,
								newBitmask));
					}
					else {
						// pass along unmodified
						keyList.add(currKey);
						valList.add(currVal);
					}
				}
			}
			if (!keyList.isEmpty() && !valList.isEmpty()) {
				final Value outputVal;
				final Key outputKey;
				if (wholeRowEncoded) {
					outputKey = new Key(
							wholeRowKey.getRow(),
							adapterId);
					outputVal = WholeRowIterator.encodeRow(
							keyList,
							valList);
				}
				else {
					outputKey = keyList.get(0);
					outputVal = valList.get(0);
				}
				output.append(
						outputKey,
						outputVal);
			}
			input.next();
		}
	}

	private Value constructNewValue(
			final Value original,
			final byte[] originalBitmask,
			final byte[] newBitmask ) {
		final byte[] newBytes = BitmaskUtils.constructNewValue(
				original.get(),
				originalBitmask,
				newBitmask);
		if (newBytes == null) {
			return null;
		}
		return new Value(
				newBytes);
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		// get fieldIds and associated adapter
		final String bitmaskStr = options.get(FIELD_SUBSET_BITMASK);
		fieldSubsetBitmask = ByteArrayUtils.byteArrayFromString(bitmaskStr);
		final String wholeRowEncodedStr = options.get(WHOLE_ROW_ENCODED_KEY);
		// default to whole row encoded if not specified
		wholeRowEncoded = ((wholeRowEncodedStr == null) || !wholeRowEncodedStr.equals(Boolean.toString(false)));
	}

	@Override
	public boolean validateOptions(
			final Map<String, String> options ) {
		if ((!super.validateOptions(options)) || (options == null)) {
			return false;
		}
		final boolean hasFieldsBitmask = options.containsKey(FIELD_SUBSET_BITMASK);
		if (!hasFieldsBitmask) {
			// all are required
			return false;
		}
		return true;
	}

	/**
	 *
	 * @return an {@link IteratorSetting} for this iterator
	 */
	public static IteratorSetting getIteratorSetting() {
		return new IteratorSetting(
				AttributeSubsettingIterator.ITERATOR_PRIORITY,
				AttributeSubsettingIterator.ITERATOR_NAME,
				AttributeSubsettingIterator.class);
	}

	/**
	 * Sets the desired subset of fields to keep
	 *
	 * @param setting
	 *            the {@link IteratorSetting}
	 * @param adapterAssociatedWithFieldIds
	 *            the adapter associated with the given fieldIds
	 * @param fieldIds
	 *            the desired subset of fieldIds
	 * @param numericDimensions
	 *            the numeric dimension fields
	 */
	public static void setFieldIds(
			final IteratorSetting setting,
			final DataAdapter<?> adapterAssociatedWithFieldIds,
			final List<String> fieldIds,
			final CommonIndexModel indexModel ) {

		final byte[] fieldSubsetBitmask = BitmaskUtils.generateFieldSubsetBitmask(
				indexModel,
				fieldIds,
				adapterAssociatedWithFieldIds);

		setting.addOption(
				FIELD_SUBSET_BITMASK,
				ByteArrayUtils.byteArrayToString(fieldSubsetBitmask));
	}
}
