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
package mil.nga.giat.geowave.core.store.index;

import com.google.common.base.Preconditions;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;

public class SecondaryIndexUtils
{

	/**
	 * constructs a composite column family consisting of the adapter prepended
	 * to the field id of the indexed attribute
	 * 
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @return byte array for use as a column family
	 */
	public static byte[] constructColumnFamily(
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId ) {
		Preconditions.checkNotNull(
				adapterId,
				"adapterId cannot be null");
		Preconditions.checkNotNull(
				indexedAttributeFieldId,
				"indexedAttributeFieldId cannot be null");
		return constructColumnFamily(
				adapterId.getBytes(),
				indexedAttributeFieldId.getBytes());
	}

	/**
	 * constructs a composite column family consisting of the adapter prepended
	 * to the field id of the indexed attribute
	 * 
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @return byte array for use as a column family
	 */
	public static byte[] constructColumnFamily(
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId ) {
		return ByteArrayUtils.combineArrays(
				adapterId,
				indexedAttributeFieldId);
	}

	/**
	 * constructs a composite column qualifier consisting of partTwo appended to
	 * partOne
	 * 
	 * @param partOne
	 * @param partTwo
	 * @return byte array for use as a column qualifier
	 */
	public static byte[] constructColumnQualifier(
			final ByteArrayId partOne,
			final ByteArrayId partTwo ) {
		Preconditions.checkNotNull(
				partOne,
				"partOne cannot be null");
		Preconditions.checkNotNull(
				partTwo,
				"partTwo cannot be null");
		return constructColumnQualifier(
				partOne.getBytes(),
				partTwo.getBytes());
	}

	/**
	 * constructs a composite column qualifier consisting of partTwo appended to
	 * partOne
	 * 
	 * @param partOne
	 * @param partTwo
	 * @return byte array for use as a column qualifier
	 */
	public static byte[] constructColumnQualifier(
			final byte[] partOne,
			final byte[] partTwo ) {
		return ByteArrayUtils.combineVariableLengthArrays(
				partOne,
				partTwo);
	}

	/**
	 * Parses and returns the primary index row id from a composite column
	 * qualifier. Useful when dealing with a secondary index of type JOIN
	 * 
	 * @param cq
	 *            a composite column qualifier
	 * @return primary index row id
	 */
	public static ByteArrayId getPrimaryRowId(
			final byte[] cq ) {
		return new ByteArrayId(
				ByteArrayUtils.splitVariableLengthArrays(
						cq).getRight());
	}

	/**
	 * parses and returns the data id from a composite column qualifier. Useful
	 * when dealing with a secondary index of type PARTIAL or FULL
	 * 
	 * @param cq
	 *            a composite column qualifier
	 * @return the data id
	 */
	public static String getDataId(
			final byte[] cq ) {
		return new ByteArrayId(
				ByteArrayUtils.splitVariableLengthArrays(
						cq).getRight()).getString();
	}

	/**
	 * parses and returns the field id from a composite column qualiifier.
	 * Useful when dealing with a secondary index of type PARTIAL or FULL
	 * 
	 * @param cq
	 *            a composite column qualifier
	 * @return the field id
	 */
	public static ByteArrayId getFieldId(
			final byte[] cq ) {
		return new ByteArrayId(
				ByteArrayUtils.splitVariableLengthArrays(
						cq).getLeft());
	}

	/**
	 * parses and returns the primary index id from a composite column
	 * qualifier. Useful when dealing with a secondary index of type JOIN
	 * 
	 * @param cq
	 *            a composite column qualifier
	 * @return the primary index id
	 */
	public static ByteArrayId getPrimaryIndexId(
			final byte[] cq ) {
		return new ByteArrayId(
				ByteArrayUtils.splitVariableLengthArrays(
						cq).getLeft());
	}

}
