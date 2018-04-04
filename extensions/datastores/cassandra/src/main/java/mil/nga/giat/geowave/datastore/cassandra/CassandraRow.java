package mil.nga.giat.geowave.datastore.cassandra;

import java.util.function.BiConsumer;

import org.apache.log4j.Logger;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.schemabuilder.Create;

import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.entities.MergeableGeoWaveRow;

public class CassandraRow extends MergeableGeoWaveRow
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
		GW_SORT_KEY(
				"sort",
				ColumnType.CLUSTER_COLUMN),
		GW_DATA_ID_KEY(
				"data_id",
				ColumnType.CLUSTER_COLUMN),
		GW_FIELD_VISIBILITY_KEY(
				"vis",
				ColumnType.CLUSTER_COLUMN),
		GW_NANO_TIME_KEY(
				"nano_time",
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

	private final Row row;

	public CassandraRow(
			final Row row ) {
		super(getFieldValues(row));
		this.row = row;
	}

	@Override
	public byte[] getDataId() {
		return row.getBytes(
				CassandraField.GW_DATA_ID_KEY.getFieldName()).array();
	}

	@Override
	public byte[] getAdapterId() {
		return row.getBytes(
				CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).array();
	}

	@Override
	public byte[] getSortKey() {
		return row.getBytes(
				CassandraField.GW_SORT_KEY.getFieldName()).array();
	}

	@Override
	public byte[] getPartitionKey() {
		return row.getBytes(
				CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
	}

	@Override
	public int getNumberOfDuplicates() {
		return row.getBytes(
				CassandraField.GW_NUM_DUPLICATES_KEY.getFieldName()).array()[0];
	}

	private static GeoWaveValue[] getFieldValues(Row row) {
		final byte[] fieldMask = row.getBytes(
				CassandraField.GW_FIELD_MASK_KEY.getFieldName()).array();
		final byte[] value = row.getBytes(
				CassandraField.GW_VALUE_KEY.getFieldName()).array();
		final byte[] visibility = row.getBytes(
				CassandraField.GW_FIELD_VISIBILITY_KEY.getFieldName()).array();

		GeoWaveValue[] fieldValues = new GeoWaveValueImpl[1];
		fieldValues[0] = new GeoWaveValueImpl(
				fieldMask,
				visibility,
				value);
		return fieldValues;
	}
}
