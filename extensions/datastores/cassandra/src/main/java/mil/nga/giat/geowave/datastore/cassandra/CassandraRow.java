package mil.nga.giat.geowave.datastore.cassandra;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.schemabuilder.Create;

import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;

public class CassandraRow implements
		NativeGeoWaveRow
{
	private static enum ColumnType {
		PARTITION_KEY(
				(
						final Create c,
						final String f ) -> c.addPartitionKey(
								f,
								DataType.blob())),
		CLUSTER_COLUMN(
				(
						final Create c,
						final String f ) -> c.addClusteringColumn(
								f,
								DataType.blob())),
		OTHER_COLUMN(
				(
						final Create c,
						final String f ) -> c.addColumn(
								f,
								DataType.blob()));
		private BiConsumer<Create, String> createFunction;

		private ColumnType(
				final BiConsumer<Create, String> createFunction ) {
			this.createFunction = createFunction;
		}
	}

	public static enum CassandraField {
		GW_PARTITION_ID_KEY(
				"P",
				ColumnType.PARTITION_KEY),
		GW_IDX_KEY(
				"X",
				ColumnType.CLUSTER_COLUMN),
		GW_ID_KEY(
				"I",
				ColumnType.OTHER_COLUMN),
		GW_VALUE_KEY(
				"V",
				ColumnType.OTHER_COLUMN);
		private final String fieldName;
		private ColumnType columnType;

		private CassandraField(
				final String fieldName,
				final ColumnType columnType ) {
			this.fieldName = fieldName;
			this.columnType = columnType;
		}

		public String getFieldName() {
			return fieldName;
		}

		public String getBindMarkerName() {
			return fieldName + "_val";
		}

		public String getLowerBoundBindMarkerName() {
			return fieldName + "_min";
		}

		public String getUpperBoundBindMarkerName() {
			return fieldName + "_max";
		}

		public void addColumn(
				final Create create ) {
			columnType.createFunction.accept(
					create,
					fieldName);
		}
	}

	private final byte[] partitionId;
	private final byte[] id;
	private final byte[] idx;
	private final byte[] value;

	public CassandraRow(
			final byte[] partitionId,
			final byte[] id,
			final byte[] idx,
			final byte[] value ) {
		this.partitionId = partitionId;
		this.id = id;
		this.idx = idx;
		this.value = value;
	}

	public CassandraRow(
			final Row row ) {
		partitionId = row.getBytes(
				CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
		id = row.getBytes(
				CassandraField.GW_ID_KEY.getFieldName()).array();
		idx = row.getBytes(
				CassandraField.GW_IDX_KEY.getFieldName()).array();
		value = row.getBytes(
				CassandraField.GW_VALUE_KEY.getFieldName()).array();
	}

	public byte[] getRawPartitionId() {
		return partitionId;
	}

	public byte[] getRawId() {
		return id;
	}

	public byte[] getRawIdx() {
		return idx;
	}

	public byte[] getRawValue() {
		return value;
	}

	public BoundStatement bindInsertion(
			final PreparedStatement insertionStatement ) {
		final BoundStatement retVal = new BoundStatement(
				insertionStatement);
		retVal.set(
				CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						partitionId),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_IDX_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						idx),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_ID_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						id),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_VALUE_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						value),
				ByteBuffer.class);
		return retVal;
	}

	@Override
	public ByteBuffer getAdapterAndDataId() {
		return ByteBuffer.wrap(
				id);
	}

	@Override
	public ByteBuffer getValue() {
		return ByteBuffer.wrap(
				value);
	}

	@Override
	public ByteBuffer getIndex() {
		return ByteBuffer.wrap(
				idx);
	}
}
