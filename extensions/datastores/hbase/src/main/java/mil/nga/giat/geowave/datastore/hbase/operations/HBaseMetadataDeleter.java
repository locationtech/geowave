package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class HBaseMetadataDeleter implements
		MetadataDeleter
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMetadataDeleter.class);

	private final HBaseOperations operations;
	private final String metadataTypeName;

	public HBaseMetadataDeleter(
			final HBaseOperations operations,
			final MetadataType metadataType ) {
		super();
		this.operations = operations;
		metadataTypeName = metadataType.name();
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		// the nature of metadata deleter is that primary ID is always
		// well-defined and it is deleting a single entry at a time
		TableName tableName = operations.getTableName(AbstractGeoWavePersistence.METADATA_TABLE);

		try {
			BufferedMutator deleter = operations.getBufferedMutator(tableName);

			Delete delete = new Delete(
					query.getPrimaryId());
			delete.addColumns(
					StringUtils.stringToBinary(metadataTypeName),
					query.getSecondaryId());

			deleter.mutate(delete);
			deleter.close();

			return true;
		}
		catch (IOException e) {
			LOGGER.error(
					"Error deleting metadata",
					e);
		}

		return false;
	}

	@Override
	public void flush() {}

}
