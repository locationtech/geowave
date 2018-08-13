package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.UnreadFieldDataList;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreOperations;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreOperations.MemoryStoreEntry;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class MapReduceMemoryOperations extends
		MemoryDataStoreOperations implements
		MapReduceDataStoreOperations
{

	private final Map<ByteArrayId, SortedSet<MemoryStoreEntry>> storeData = Collections
			.synchronizedMap(new HashMap<ByteArrayId, SortedSet<MemoryStoreEntry>>());

	@Override
	public <T> Reader<T> createReader(
			RecordReaderParams<T> readerParams ) {

		ByteArrayId partitionKey = new ByteArrayId(
				readerParams.getRowRange().getPartitionKey() == null ? new byte[0] : readerParams
						.getRowRange()
						.getPartitionKey());

		ByteArrayRange sortRange = new ByteArrayRange(
				new ByteArrayId(
						readerParams.getRowRange().getStartSortKey() == null ? new byte[0] : readerParams
								.getRowRange()
								.getStartSortKey()),
				new ByteArrayId(
						readerParams.getRowRange().getEndSortKey() == null ? new byte[0] : readerParams
								.getRowRange()
								.getEndSortKey()));

		return createReader((ReaderParams) new ReaderParams(
				readerParams.getIndex(),
				readerParams.getAdapterStore(),
				readerParams.getAdapterIds(),
				readerParams.getMaxResolutionSubsamplingPerDimension(),
				readerParams.getAggregation(),
				readerParams.getFieldSubsets(),
				readerParams.isMixedVisibility(),
				readerParams.isServersideAggregation(),
				false,
				new QueryRanges(
						Collections.singleton(new SinglePartitionQueryRanges(
								partitionKey,
								Collections.singleton(sortRange)))),
				readerParams.getFilter(),
				readerParams.getLimit(),
				readerParams.getMaxRangeDecomposition(),
				readerParams.getCoordinateRanges(),
				readerParams.getConstraints(),
				readerParams.getRowTransformer(),
				readerParams.getAdditionalAuthorizations()));

	}

}
