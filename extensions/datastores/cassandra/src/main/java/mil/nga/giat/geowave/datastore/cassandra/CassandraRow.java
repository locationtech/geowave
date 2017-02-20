package mil.nga.giat.geowave.datastore.cassandra;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.schemabuilder.Create;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;

public class CassandraRow extends
		GeoWaveKeyImpl
{
	private final static Logger LOGGER = Logger.getLogger(
			CassandraRow.class);

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
				"partition",
				ColumnType.PARTITION_KEY),
		GW_ADAPTER_ID_KEY(
				"adapter_id",
				ColumnType.CLUSTER_COLUMN),
		GW_IDX_KEY(
				"idx",
				ColumnType.CLUSTER_COLUMN),
		GW_DATA_ID_KEY(
				"data_id",
				ColumnType.CLUSTER_COLUMN),
		GW_FIELD_MASK_KEY(
				"field_mask",
				ColumnType.OTHER_COLUMN),
		GW_VALUE_KEY(
				"value",
				ColumnType.OTHER_COLUMN),
		GW_NUM_DUPLICATES_KEY(
				"num_duplicates",
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

	public CassandraRow(
			final byte[] partitionId,
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] idx,
			final byte[] fieldMask,
			final byte[] value,
			final int numDuplicates ) {
		super(
				dataId,
				adapterId,
				idx,
				fieldMask,
				value,
				numDuplicates);

		this.partitionId = partitionId;
	}

	public CassandraRow(
			final Row row ) {
		super(
				row.getBytes(
						CassandraField.GW_DATA_ID_KEY.getFieldName()).array(),
				row.getBytes(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).array(),
				row.getBytes(
						CassandraField.GW_IDX_KEY.getFieldName()).array(),
				row.getBytes(
						CassandraField.GW_FIELD_MASK_KEY.getFieldName()).array(),
				row.getBytes(
						CassandraField.GW_VALUE_KEY.getFieldName()).array(),
				(int)(row.getBytes(
						CassandraField.GW_NUM_DUPLICATES_KEY.getFieldName()).get(0)));
		
		partitionId = row.getBytes(
				CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
	}

	public byte[] getPartitionId() {
		return partitionId;
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
						index),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_DATA_ID_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						dataId),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_FIELD_MASK_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						fieldMask),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						adapterId),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_VALUE_KEY.getBindMarkerName(),
				ByteBuffer.wrap(
						value),
				ByteBuffer.class);
		retVal.set(
				CassandraField.GW_NUM_DUPLICATES_KEY.getBindMarkerName(),
				ByteBuffer.wrap(new byte[] { (byte)numberOfDuplicates }),
				ByteBuffer.class);
		return retVal;
	}
}
