package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.Base64;

import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;

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
