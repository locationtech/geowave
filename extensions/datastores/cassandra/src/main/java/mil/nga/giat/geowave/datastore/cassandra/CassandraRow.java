package mil.nga.giat.geowave.datastore.cassandra;

import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;
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
						final Pair<String,DataType> f ) -> c.addPartitionKey(
								f.getLeft(),
								f.getRight())),
		CLUSTER_COLUMN(
				(
						final Create c,
						final Pair<String,DataType> f ) -> c.addClusteringColumn(
								f.getLeft(),
								f.getRight())),
		OTHER_COLUMN(
				(
						final Create c,
						final Pair<String,DataType> f ) -> c.addColumn(
								f.getLeft(),
								f.getRight()));

		private BiConsumer<Create, Pair<String,DataType>> createFunction;

		private ColumnType(
				final BiConsumer<Create, Pair<String,DataType>> createFunction ) {
			this.createFunction = createFunction;
		}
	}

	public static enum CassandraField {
		GW_PARTITION_ID_KEY(
				"partition",
				DataType.blob(),
				ColumnType.PARTITION_KEY),
		GW_ADAPTER_ID_KEY(
				"adapter_id",
				DataType.smallint(),
				ColumnType.CLUSTER_COLUMN),
		GW_SORT_KEY(
				"sort",
				DataType.blob(),
				ColumnType.CLUSTER_COLUMN),
		GW_DATA_ID_KEY(
				"data_id",
				DataType.blob(),
				ColumnType.CLUSTER_COLUMN),
		GW_FIELD_VISIBILITY_KEY(
				"vis",
				DataType.blob(),
				ColumnType.CLUSTER_COLUMN),
		GW_NANO_TIME_KEY(
				"nano_time",
				DataType.blob(),
				ColumnType.CLUSTER_COLUMN),
		GW_FIELD_MASK_KEY(
				"field_mask",
				DataType.blob(),
				ColumnType.OTHER_COLUMN),
		GW_VALUE_KEY(
				"value",
				DataType.blob(),
				ColumnType.OTHER_COLUMN),
		GW_NUM_DUPLICATES_KEY(
				"num_duplicates",
				DataType.tinyint(),
				ColumnType.OTHER_COLUMN);

		private final String fieldName;
		private final DataType dataType;
		private ColumnType columnType;

		private CassandraField(
				final String fieldName,
				final DataType dataType,
				final ColumnType columnType ) {
			this.fieldName = fieldName;
			this.dataType = dataType;
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
					Pair.of(fieldName,dataType));
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
		return (int) row.getByte(CassandraField.GW_NUM_DUPLICATES_KEY.getFieldName());
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

	@Override
	public short getInternalAdapterId() {
		return row.getShort(CassandraField.GW_ADAPTER_ID_KEY.getFieldName());
	}
}
