package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.CastIterator;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class HBaseSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore<RowMutations>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSecondaryIndexDataStore.class);
	private final BasicHBaseOperations hbaseOperations;
	@SuppressWarnings("unused")
	private final HBaseOptions hbaseOptions;
	private DataStore dataStore = null;

	public HBaseSecondaryIndexDataStore(
			final BasicHBaseOperations hbaseOperations ) {
		this(
				hbaseOperations,
				new HBaseOptions());
	}

	public HBaseSecondaryIndexDataStore(
			final BasicHBaseOperations hbaseOperations,
			final HBaseOptions hbaseOptions ) {
		super();
		this.hbaseOperations = hbaseOperations;
		this.hbaseOptions = hbaseOptions;
	}

	@Override
	public void setDataStore(
			final DataStore dataStore ) {
		this.dataStore = dataStore;
	}

	@Override
	protected Writer<RowMutations> getWriter(
			final ByteArrayId secondaryIndexId ) {
		final String secondaryIndexName = secondaryIndexId.getString();
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		HBaseWriter writer = null;
		try {
			writer = hbaseOperations.createWriter(
					secondaryIndexName,
					new String[] {},
					false);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to create HBase Writer.",
					e);
			return null;
		}
		writerCache.put(
				secondaryIndexName,
				writer);

		return writer;
	}

	@Override
	protected RowMutations buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId,
			final byte[] attributeVisibility )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);
		final Put p = new Put(
				secondaryIndexRowId);
		p.setCellVisibility(new CellVisibility(
				StringUtils.stringFromBinary(attributeVisibility)));
		p.addColumn(
				SecondaryIndexUtils.constructColumnFamily(
						adapterId,
						indexedAttributeFieldId),
				SecondaryIndexUtils.constructColumnQualifier(
						primaryIndexId,
						primaryIndexRowId),
				EMPTY_VALUE);
		m.add(p);
		return m;
	}

	@Override
	protected RowMutations buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);
		final Put p = new Put(
				secondaryIndexRowId);
		p.setCellVisibility(new CellVisibility(
				StringUtils.stringFromBinary(fieldVisibility)));
		p.addColumn(
				SecondaryIndexUtils.constructColumnFamily(
						adapterId,
						indexedAttributeFieldId),
				SecondaryIndexUtils.constructColumnQualifier(
						fieldId,
						dataId),
				fieldValue);
		m.add(p);
		return m;
	}

	@Override
	protected RowMutations buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);
		final Delete d = new Delete(
				secondaryIndexRowId);
		d.addColumns(
				SecondaryIndexUtils.constructColumnFamily(
						adapterId,
						indexedAttributeFieldId),
				SecondaryIndexUtils.constructColumnQualifier(
						primaryIndexId,
						primaryIndexRowId));
		m.add(d);
		return m;
	}

	@Override
	protected RowMutations buildFullDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);
		final Delete d = new Delete(
				secondaryIndexRowId);
		d.addColumn(
				SecondaryIndexUtils.constructColumnFamily(
						adapterId,
						indexedAttributeFieldId),
				SecondaryIndexUtils.constructColumnQualifier(
						fieldId,
						dataId));
		m.add(d);
		return m;
	}

	@Override
	public <T> CloseableIterator<T> query(
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final DataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations ) {
		final List<Scan> scans = new ArrayList<Scan>();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				adapter.getAdapterId(),
				indexedAttributeFieldId);
		final List<ByteArrayRange> scanRanges = query.getSecondaryIndexConstraints(secondaryIndex);
		for (final ByteArrayRange scanRange : scanRanges) {
			final Scan scan = new Scan();
			scan.addFamily(columnFamily);
			scan.setStartRow(scanRange.getStart().getBytes());
			scan.setStopRow((scanRange.isSingleValue()) ? scanRange.getStart().getBytes() : scanRange
					.getEnd()
					.getBytes());
			scans.add(scan);
		}
		final List<ResultScanner> results = new ArrayList<ResultScanner>();
		for (final Scan scan : scans) {
			try {
				final ResultScanner resultScanner = hbaseOperations.getScannedResults(
						scan,
						secondaryIndex.getId().getString(),
						authorizations);
				if (resultScanner != null) {
					results.add(resultScanner);
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Could not get the results from scanner ",
						e);
			}
		}
		if (!results.isEmpty()) {
			final List<CloseableIterator<Object>> allResultsList = new ArrayList<>();
			for (final ResultScanner resultsScan : results) {
				if (secondaryIndex.getSecondaryIndexType().equals(
						SecondaryIndexType.JOIN)) {
					final List<CloseableIterator<Object>> allResults = new ArrayList<>();
					try (final CloseableIterator<Pair<ByteArrayId, ByteArrayId>> joinEntryIterator = new HBaseSecondaryIndexJoinEntryIteratorWrapper<T>(
							resultsScan,
							columnFamily,
							adapter)) {
						while (joinEntryIterator.hasNext()) {
							final Pair<ByteArrayId, ByteArrayId> entry = joinEntryIterator.next();
							final ByteArrayId primaryIndexId = entry.getLeft();
							final ByteArrayId primaryIndexRowId = entry.getRight();
							final CloseableIterator<Object> intermediateResults = dataStore.query(
									new QueryOptions(
											adapter.getAdapterId(),
											primaryIndexId),
									new RowIdQuery(
											primaryIndexRowId));
							allResults.add(intermediateResults);
							final CloseableIterator<Object> intermediateResultsWrapper = new CloseableIteratorWrapper<Object>(
									new Closeable() {
										@Override
										public void close()
												throws IOException {
											for (CloseableIterator<Object> resultIter : allResults) {
												resultIter.close();
											}
										}
									},
									Iterators.concat(new CastIterator<Object>(
											allResults.iterator())));
							allResultsList.add(intermediateResultsWrapper);
						}
					}
					catch (final IOException e) {
						LOGGER.error(
								"Could not close iterator",
								e);
					}
				}
				else {
					allResultsList.add(new HBaseSecondaryIndexEntryIteratorWrapper<T>(
							resultsScan,
							columnFamily,
							adapter,
							primaryIndex));
				}
			}
			return new CloseableIteratorWrapper<T>(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							for (final CloseableIterator<Object> closeableIterator : allResultsList) {
								closeableIterator.close();
							}
						}
					},
					Iterators.concat(new CastIterator<T>(
							allResultsList.iterator())));
		}
		return new CloseableIterator.Empty<T>();
	}

}