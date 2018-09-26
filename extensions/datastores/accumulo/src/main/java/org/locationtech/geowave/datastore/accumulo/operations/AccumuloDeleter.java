package org.locationtech.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.BatchDeleter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.Deleter;

public class AccumuloDeleter<T> extends
		AccumuloReader<T> implements
		Deleter<T>
{

	// private final static Logger LOGGER = LoggerFactory
	// .getLogger(
	// AccumuloDeleter.class);

	// private Map<ByteArrayId, Short> idMap;
	// private Map<ByteArrayId, Integer> dupCountMap;
	// private AccumuloOperations operations;
	// private ReaderParams<T> readerParams;

	private boolean closed = false;

	public AccumuloDeleter(
			final BatchDeleter scanner,
			final GeoWaveRowIteratorTransformer<T> transformer,
			final int partitionKeyLength,
			final boolean wholeRowEncoding,
			final boolean clientSideRowMerging,
			final boolean parallel ) {
		// AccumuloOperations operations,
		// ReaderParams<T> readerParams ) {
		super(
				scanner,
				transformer,
				partitionKeyLength,
				wholeRowEncoding,
				clientSideRowMerging,
				parallel);
		// idMap = new HashMap<>();
		// dupCountMap = new HashMap<>();
		// this.operations = operations;
		// this.readerParams = readerParams;
	}

	@Override
	public void close()
			throws Exception {
		if (!closed) {
			// make sure delete is only called once
			((BatchDeleter) scanner).delete();

			closed = true;
		}
		super.close();
		// We should add an IT that has a time range that crosses dec-january
		// and then delete by time with just dec or just january to fail this

		// Have to delete dups by data ID if any
		// if (!dupCountMap.isEmpty()) {
		// LOGGER
		// .warn(
		// "Need to delete duplicates by data ID");
		// int dupDataCount = 0;
		// int dupFailCount = 0;
		// boolean deleteByIdSuccess = true;
		//
		// for (final ByteArrayId dataId : dupCountMap.keySet()) {
		// if (!operations
		// .createdelete(
		// new QueryOptions(),
		// new DataIdQuery(
		// idMap
		// .get(
		// dataId),
		// dataId))) {
		// deleteByIdSuccess = false;
		// dupFailCount++;
		// }
		// else {
		// dupDataCount++;
		// }
		// }
		//
		// if (deleteByIdSuccess) {
		// LOGGER
		// .warn(
		// "Deleted " + dupDataCount + " duplicates by data ID");
		// }
		// else {
		// LOGGER
		// .warn(
		// "Failed to delete " + dupFailCount + " duplicates by data ID");
		// }
		// }
		// Not sure why we need this but it was being done in v0.9.7
		// final int undeleted = getCount(
		// indexAdapterPairs,
		// sanitizedQueryOptions,
		// sanitizedQuery);
		// if (undeleted > 0) {
		// final RowDeleter rowDeleter = operations.createDeleter(
		// readerParams.getIndex().getId(),
		// readerParams.getAdditionalAuthorizations());
		// if (rowDeleter != null) {
		// try (Deleter<T> i = new QueryAndDeleteByRow<>(
		// rowDeleter,
		// operations.createReader(
		// readerParams))) {
		// while (i.hasNext()) {
		// i.next();
		// }
		// }
		// }
		// }

	}

	// protected int getCount(
	// final List<Pair<PrimaryIndex, List<DataAdapter<Object>>>>
	// indexAdapterPairs,
	// final QueryOptions sanitizedQueryOptions,
	// final Query sanitizedQuery ) {
	// int count = 0;
	//
	// for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair
	// : indexAdapterPairs) {
	// for (final DataAdapter dataAdapter : indexAdapterPair.getRight()) {
	// final QueryOptions countOptions = new QueryOptions(
	// sanitizedQueryOptions);
	// countOptions
	// .setAggregation(
	// new CountAggregation(),
	// dataAdapter);
	//
	// try (final CloseableIterator<Object> it = query(
	// countOptions,
	// sanitizedQuery)) {
	// while (it.hasNext()) {
	// if (countAggregation) {
	// final CountResult result = ((CountResult) (it.next()));
	// if (result != null) {
	// count += result.getCount();
	// }
	// }
	// else {
	// it.next();
	// count++;
	// }
	// }
	//
	// it.close();
	// }
	// catch (final Exception e) {
	// LOGGER
	// .warn(
	// "Error running count aggregation",
	// e);
	// return count;
	// }
	// }
	// }
	//
	// return count;
	// }

	@Override
	public void entryScanned(
			final T entry,
			final GeoWaveRow row ) {
		// final int rowDups = row.getNumberOfDuplicates();
		//
		// if (rowDups > 0) {
		// final ByteArrayId dataId = new ByteArrayId(
		// row.getDataId());
		// if (idMap
		// .get(
		// dataId) == null) {
		// idMap
		// .put(
		// dataId,
		// row.getInternalAdapterId());
		// }
		//
		// final Integer mapDups = dupCountMap
		// .get(
		// dataId);
		//
		// if (mapDups == null) {
		// dupCountMap
		// .put(
		// dataId,
		// rowDups);
		// }
		// else if (mapDups == 1) {
		// dupCountMap
		// .remove(
		// dataId);
		// }
		// else {
		// dupCountMap
		// .put(
		// dataId,
		// mapDups - 1);
		// }
		// }

	}
}
