package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.io.Closeable;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

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

}
