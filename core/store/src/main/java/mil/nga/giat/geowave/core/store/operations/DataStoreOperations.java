package mil.nga.giat.geowave.core.store.operations;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface DataStoreOperations
{

	public boolean indexExists(
			ByteArrayId indexId )
			throws IOException;

	public boolean metadataExists(
			MetadataType type )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public boolean deleteAll(
			ByteArrayId indexId,
			Short adapterId,
			String... additionalAuthorizations );

	public boolean insureAuthorizations(
			String clientUser,
			String... authorizations );

	/**
	 * Creates a new writer that can be used by an index.
	 *
	 * @param indexId
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param adapterId
	 *            The name of the adapter.
	 * @param options
	 *            basic options available
	 * @param splits
	 *            If the table is created, these splits will be added as
	 *            partition keys. Null can be used to imply not to add any
	 *            splits.
	 * @return The appropriate writer
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Writer createWriter(
			ByteArrayId indexId,
			short internalAdapterId );

	public MetadataWriter createMetadataWriter(
			MetadataType metadataType );

	public MetadataReader createMetadataReader(
			MetadataType metadataType );

	public MetadataDeleter createMetadataDeleter(
			MetadataType metadataType );

	public Reader createReader(
			ReaderParams readerParams );

	public Deleter createDeleter(
			ByteArrayId indexId,
			String... authorizations )
			throws Exception;

	public boolean mergeData(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore );
}
