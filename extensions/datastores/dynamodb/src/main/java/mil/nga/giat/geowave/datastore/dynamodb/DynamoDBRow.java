package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;

public class DynamoDBRow implements
		NativeGeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_ID_KEY = "I";
	public static final String GW_IDX_KEY = "X";
	public static final String GW_VALUE_KEY = "V";

	private final Map<String, AttributeValue> objMap;

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		this.objMap = objMap;
	}

	@Override
	public ByteBuffer getAdapterAndDataId() {
		return objMap.get(
				GW_ID_KEY).getB();
	}

	@Override
	public ByteBuffer getValue() {
		return objMap.get(
				GW_VALUE_KEY).getB();
	}

	@Override
	public ByteBuffer getIndex() {
		return objMap.get(
				GW_IDX_KEY).getB();
	}
}
