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
package org.locationtech.geowave.datastore.dynamodb.util;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;

import org.locationtech.geowave.datastore.dynamodb.operations.DynamoDBOperations;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.Base64;

public class DynamoDBUtils
{
	public static class NoopClosableIteratorWrapper implements
			Closeable
	{
		public NoopClosableIteratorWrapper() {}

		@Override
		public void close() {}
	}

	public static byte[] getPrimaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(DynamoDBOperations.METADATA_PRIMARY_ID_KEY);
		if (v != null) {
			return v.getB().array();
		}
		return null;
	}

	public static byte[] getSecondaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(DynamoDBOperations.METADATA_SECONDARY_ID_KEY);
		if (v != null) {
			return v.getB().array();
		}
		return null;
	}

	public static byte[] getVisibility(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(DynamoDBOperations.METADATA_VISIBILITY_KEY);
		if (v != null) {
			return v.getB().array();
		}
		return null;
	}

	public static byte[] getValue(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(DynamoDBOperations.METADATA_VALUE_KEY);
		if (v != null) {
			return v.getB().array();
		}
		return null;
	}

	private static final String BASE64_DEFAULT_ENCODING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
	private static final String BASE64_SORTABLE_ENCODING = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz=";

	private static final byte[] defaultToSortable = new byte[127];
	private static final byte[] sortableToDefault = new byte[127];
	static {
		Arrays.fill(
				defaultToSortable,
				(byte) 0);
		Arrays.fill(
				sortableToDefault,
				(byte) 0);
		for (int i = 0; i < BASE64_DEFAULT_ENCODING.length(); i++) {
			defaultToSortable[BASE64_DEFAULT_ENCODING.charAt(i)] = (byte) (BASE64_SORTABLE_ENCODING.charAt(i) & 0xFF);
			sortableToDefault[BASE64_SORTABLE_ENCODING.charAt(i)] = (byte) (BASE64_DEFAULT_ENCODING.charAt(i) & 0xFF);
		}
	}

	public static byte[] encodeSortableBase64(
			final byte[] original ) {
		final byte[] bytes = Base64.encode(original);
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = defaultToSortable[bytes[i]];
		}
		return bytes;
	}

	public static byte[] decodeSortableBase64(
			final byte[] original ) {
		final byte[] bytes = new byte[original.length];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = sortableToDefault[original[i]];
		}
		return Base64.decode(bytes);
	}

}
