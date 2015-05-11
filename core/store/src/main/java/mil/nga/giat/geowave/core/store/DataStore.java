package mil.nga.giat.geowave.core.store;

import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

/**
 * A DataStore can both ingest and query data based on persisted indices and
 * data adapters. When the data is ingested it is explicitly given an index and
 * a data adapter which is then persisted to be used in subsequent queries.
 */
public interface DataStore
{
	/**
	 * Returns an index writer to perform batched write operations
	 * 
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @return Returns the index writer which can be used for batch write
	 *         operations
	 */
	public <T> IndexWriter createIndexWriter(
			Index index );

	/**
	 * Ingests a single entry into the data store
	 * 
	 * @param writableAdapter
	 *            The writable adapter that allows the data store to translate
	 *            the entry into a persistable format
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @param entry
	 *            The entry to ingest
	 * @return Returns all row IDs for which the entry is ingested
	 */
	public <T> List<ByteArrayId> ingest(
			final WritableDataAdapter<T> writableAdapter,
			Index index,
			T entry );

	/**
	 * Ingests a collection of entries into the data store described by an
	 * iterator on the entries
	 * 
	 * @param writableAdapter
	 *            The writable adapter that allows the data store to translate
	 *            the entries into a persistable format
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @param entryIterator
	 *            An iterator on the entries to ingest
	 */
	public <T> void ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final Iterator<T> entryIterator );

	/**
	 * Ingests a single entry into the data store
	 * 
	 * @param writableAdapter
	 *            The writable adapter that allows the data store to translate
	 *            the entry into a persistable format
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @param entry
	 *            The entry to ingest
	 * @param customFieldVisibilityWriter
	 *            per entry visibility control
	 * @return Returns all row IDs for which the entry is ingested
	 */
	public <T> List<ByteArrayId> ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter );

	/**
	 * Ingests a collection of entries into the data store described by an
	 * iterator on the entries
	 * 
	 * @param writableAdapter
	 *            The writable adapter that allows the data store to translate
	 *            the entries into a persistable format
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @param entryIterator
	 *            An iterator on the entries to ingest
	 * @param ingestCallback
	 *            A callback method to receive feedback such as row IDs for
	 *            every entry that is successfully ingested
	 */
	public <T> void ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback );

	/**
	 * Ingests a collection of entries into the data store described by an
	 * iterator on the entries
	 * 
	 * @param writableAdapter
	 *            The writable adapter that allows the data store to translate
	 *            the entries into a persistable format
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @param entryIterator
	 *            An iterator on the entries to ingest
	 * @param ingestCallback
	 *            A callback method to receive feedback such as row IDs for
	 *            every entry that is successfully ingested
	 * @param customFieldVisibilityWriter
	 *            per entry visibility control
	 */
	public <T> void ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter );

	/**
	 * Returns all data in this data store that matches the query parameter. All
	 * indices that are supported by the query will be queried and all data
	 * types that match the query will be returned as an instance of the native
	 * data type that was originally ingested.
	 * 
	 * @param query
	 *            The description of the query to be performed
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public CloseableIterator<?> query(
			final Query query );

	/**
	 * Returns the data element associated with the given row ID stored in the
	 * given index
	 * 
	 * @param index
	 *            The index to search for the entry.
	 * @param rowId
	 *            The full row ID to use as the query.
	 * 
	 * @return The entry that was ingested with the given row ID. This row ID is
	 *         the one assigned to the entry on ingest into the given index.
	 *         Null is returned if the row ID does not match any entries in the
	 *         data store for the given index.
	 */
	@Deprecated
	public <T> T getEntry(
			final Index index,
			final ByteArrayId rowId );

	/**
	 * Returns the data element associated with the given data ID and adapter ID
	 * stored in the given index
	 * 
	 * @param index
	 *            The index to search for the entry.
	 * @param dataId
	 *            The data ID to use for the query.
	 * @param adapterId
	 *            The adapter ID to use for the query.
	 * @param authorizations
	 *            additional authorizations to fetch the entry
	 * 
	 * @return The entry that was ingested with the given data ID and adapter
	 *         ID. This combination of data ID and adapter ID is the one
	 *         assigned to the entry on ingest into the given index. Null is
	 *         returned if the data ID and adapter ID do not match any entries
	 *         in the data store for the given index.
	 */
	public <T> T getEntry(
			final Index index,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final String... additionalAuthorizations );

	/**
	 * Deletes the data element associated with the given data ID and adapter ID
	 * stored in the given index
	 * 
	 * @param index
	 *            The index to search for the entry.
	 * @param dataId
	 *            The data ID to use for the query.
	 * @param adapterId
	 *            The adapter ID to use for the query.
	 * @param authorizations
	 *            additional authorizations to delete the entry
	 * @return Returns true if the entry was found and deleted successfully.
	 *         Returns false if the entry could not be found, if the entry could
	 *         not be deleted, or if the entry in the alternate index could not
	 *         be deleted.
	 */
	public boolean deleteEntry(
			final Index index,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final String... authorizations );

	/**
	 * Returns all data with the given row ID prefix stored in the given index
	 * 
	 * @param index
	 *            The index to search for the entry.
	 * @param rowPrefix
	 *            A prefix for the row ID to use as the query.
	 * @param additionalAuthorizations
	 *            additional authorizations to any data store specific defaults
	 * 
	 * @return All entries that were ingested with a row ID that is prefixed by
	 *         the given rowPrefix. The "row ID" is the one assigned to the
	 *         entry on ingest into the given index.
	 */
	public <T> CloseableIterator<T> getEntriesByPrefix(
			final Index index,
			final ByteArrayId rowPrefix,
			final String... authorizations );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches the adapter (the same adapter ID as the ID ingested). All indices
	 * that are supported by the query will be queried and returned as an
	 * instance of the native data type that this adapter supports.
	 * 
	 * @param adapter
	 *            the data adapter to use for the query
	 * @param query
	 *            The description of the query to be performed
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			final Query query );

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in. All data types that
	 * match the query will be returned as an instance of the native data type
	 * that was originally ingested.
	 * 
	 * @param index
	 *            The index information to query against. All data within the
	 *            index of this index ID will be queried and returned.
	 * @param query
	 *            The description of the query to be performed
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			Index index,
			final Query query );

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in. All data types that
	 * match the query will be returned as an instance of the native data type
	 * that was originally ingested. Additional query options will be applied to
	 * results
	 * 
	 * @param index
	 *            The index information to query against. All data within the
	 *            index of this index ID will be queried and returned.
	 * @param query
	 *            The description of the query to be performed
	 * @param queryOptions
	 *            Additional options to be applied to the query results
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			Index index,
			final Query query,
			final QueryOptions queryOptions );

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in and matches the adapter
	 * (the same adapter ID as the ID ingested). All data that matches the
	 * query, adapter ID, and is in the index ID will be returned as an instance
	 * of the native data type that this adapter supports.
	 * 
	 * @param adapter
	 *            the data adapter to use for the query
	 * @param index
	 *            The index information to query against. All data within the
	 *            index of this index ID will be queried and returned.
	 * @param query
	 *            The description of the query to be performed
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			final Query query );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches one of the adapter IDs. All data types that match the query and
	 * one of the adapter IDs will be returned as an instance of the native data
	 * type that was originally ingested.
	 * 
	 * @param adapterIds
	 *            The data adapter IDs to use for the query - only data that
	 *            matches one of these adapter IDs will be returned
	 * @param query
	 *            The description of the query to be performed
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public CloseableIterator<?> query(
			List<ByteArrayId> adapterIds,
			final Query query );

	/**
	 * Returns all data in this data store that matches the query parameter. All
	 * indices that are supported by the query will be queried and all data
	 * types that match the query will be returned as an instance of the native
	 * data type that was originally ingested. The iterator will only return as
	 * many results as the limit passed in.
	 * 
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public CloseableIterator<?> query(
			final Query query,
			final int limit );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches the adapter (the same adapter ID as the ID ingested). All indices
	 * that are supported by the query will be queried and returned as an
	 * instance of the native data type that this adapter supports. The iterator
	 * will only return as many results as the limit passed in.
	 * 
	 * @param adapter
	 *            the data adapter to use for the query
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			final Query query,
			final int limit );

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in. All data types that
	 * match the query will be returned as an instance of the native data type
	 * that was originally ingested. The iterator will only return as many
	 * results as the limit passed in.
	 * 
	 * @param index
	 *            The index information to query against. All data within the
	 *            index of this index ID will be queried and returned.
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			Index index,
			final Query query,
			final int limit );

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in and matches the adapter
	 * (the same adapter ID as the ID ingested). All data that matches the
	 * query, adapter ID, and is in the index ID will be returned as an instance
	 * of the native data type that this adapter supports. The iterator will
	 * only return as many results as the limit passed in.
	 * 
	 * @param adapter
	 *            the data adapter to use for the query
	 * @param index
	 *            The index information to query against. All data within the
	 *            index of this index ID will be queried and returned.
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			final Query query,
			final int limit );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches one of the adapter IDs. All data types that match the query and
	 * one of the adapter IDs will be returned as an instance of the native data
	 * type that was originally ingested. The iterator will only return as many
	 * results as the limit passed in.
	 * 
	 * @param adapterIds
	 *            The data adapter IDs to use for the query - only data that
	 *            matches one of these adapter IDs will be returned
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public CloseableIterator<?> query(
			List<ByteArrayId> adapterIds,
			final Query query,
			final int limit );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches one of the adapter IDs. All data types that match the query and
	 * one of the adapter IDs will be returned as an instance of the native data
	 * type that was originally ingested. The iterator will only return as many
	 * results as the limit passed in.
	 * 
	 * @param adapterIds
	 *            The data adapter IDs to use for the query - only data that
	 *            matches one of these adapter IDs will be returned
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @param authorization
	 *            The authorization used to override the default authorization
	 *            for cell visibility.
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final int limit,
			final String... authorizations );

	/**
	 * Returns all data in this data store that matches the query parameter and
	 * matches one of the adapter IDs. All data types that match the query and
	 * one of the adapter IDs will be returned as an instance of the native data
	 * type that was originally ingested. The iterator will only return as many
	 * results as the limit passed in.
	 * 
	 * @param adapterIds
	 *            The data adapter IDs to use for the query - only data that
	 *            matches one of these adapter IDs will be returned
	 * @param query
	 *            The description of the query to be performed
	 * @param limit
	 *            The maximum number of entries to return
	 * @param scanCallback
	 *            A callback invoked for each row with full row information.
	 * @param authorizations
	 *            The authorization used to override the default authorization
	 *            for cell visibility.
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final Integer limit,
			final ScanCallback<?> scanCallback,
			final String... authorizations );
}
