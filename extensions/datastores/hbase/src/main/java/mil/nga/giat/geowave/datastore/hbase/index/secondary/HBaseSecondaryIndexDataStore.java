package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.Closable;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseSecondaryIndexDataStore implements
		SecondaryIndexDataStore,
		Closable
{
	private final static Logger LOGGER = Logger.getLogger(HBaseSecondaryIndexDataStore.class);
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final BasicHBaseOperations hbaseOperations;
	private final Map<String, HBaseWriter> writerCache = new HashMap<>();

	public HBaseSecondaryIndexDataStore(
			final BasicHBaseOperations hbaseOperations ) {
		super();
		this.hbaseOperations = hbaseOperations;
	}

	private HBaseWriter getWriter(
			final SecondaryIndex<?> secondaryIndex )
			throws IOException {
		final String secondaryIndexName = secondaryIndex.getIndexStrategy().getId();
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		HBaseWriter writer = null;
		writer = hbaseOperations.createWriter(
				TABLE_PREFIX + secondaryIndexName,
				StringUtils.stringFromBinary(secondaryIndex.getId().getBytes()),
				true);
		writerCache.put(
				secondaryIndexName,
				writer);

		return writer;
	}

	@Override
	public void store(
			final SecondaryIndex<?> secondaryIndex,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId,
			final List<FieldInfo<?>> indexedAttributes ) {
		try {
			final HBaseWriter writer = getWriter(secondaryIndex);
			if (writer != null) {
				for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
					@SuppressWarnings("unchecked")
					final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex
							.getIndexStrategy()
							.getInsertionIds(
									Arrays.asList(indexedAttribute));
					for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
						writer.write(buildMutation(
								insertionId.getBytes(),
								secondaryIndex.getId().getBytes(),
								indexedAttribute.getDataValue().getId().getBytes(),
								indexedAttribute.getWrittenValue(),
								indexedAttribute.getVisibility(),
								primaryIndexId.getBytes(),
								primaryIndexRowId.getBytes()));
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error("Failed to store to secondary index: " + e);
		}
	}

	@Override
	public void delete(
			final SecondaryIndex<?> secondaryIndex,
			final List<FieldInfo<?>> indexedAttributes ) {
		try {
			final HBaseWriter writer = getWriter(secondaryIndex);
			if (writer != null) {
				for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
					@SuppressWarnings("unchecked")
					final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex
							.getIndexStrategy()
							.getInsertionIds(
									Arrays.asList(indexedAttribute));
					for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
						writer.write(buildDeleteMutation(
								insertionId.getBytes(),
								secondaryIndex.getId().getBytes(),
								indexedAttribute.getDataValue().getId().getBytes()));
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error("Failed to delete from secondary index: " + e);
		}
	}

	@Override
	public void removeAll() {
		close();
		writerCache.clear();
	}

	private RowMutations buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName,
			final byte[] attributeValue,
			final byte[] visibility,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);

		final Put p = new Put(
				secondaryIndexRowId);
		p.setCellVisibility(new CellVisibility(
				StringUtils.stringFromBinary(visibility)));
		p.addColumn(
				secondaryIndexId,
				attributeName,
				attributeValue);

		m.add(p);

		return m;
	}

	private RowMutations buildDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName )
			throws IOException {
		final RowMutations m = new RowMutations(
				secondaryIndexRowId);

		final Delete d = new Delete(
				secondaryIndexRowId);
		d.addColumn(
				secondaryIndexId,
				attributeName);

		m.add(d);

		return m;
	}

	@Override
	public CloseableIterator<ByteArrayId> query(
			final SecondaryIndex<?> secondaryIndex,
			final List<ByteArrayRange> ranges,
			final List<DistributableQueryFilter> constraints,
			final ByteArrayId primaryIndexId,
			final String... visibility ) {

		final List<Scan> scans = new ArrayList<Scan>();

		// final Scanner scanner = getScanner(
		// secondaryIndex.getIndexStrategy().getId(),
		// visibility);

		// scanner.addScanIterator(getScanIteratorSettings(
		// constraints,
		// primaryIndexId));

		// TODO visibility

		for (final ByteArrayRange range : ranges) {
			final Scan scanner = new Scan();
			if (range.getStart() != null) {
				scanner.setStartRow(range.getStart().getBytes());
				if (!range.isSingleValue()) {
					scanner.setStopRow(HBaseUtils.getNextPrefix(range.getEnd().getBytes()));
				}
				else {
					scanner.setStopRow(HBaseUtils.getNextPrefix(range.getStart().getBytes()));
				}
			}

			scans.add(scanner);
		}

		final List<ResultScanner> results = new ArrayList<ResultScanner>();

		// TODO Consider parallelization as list of scanners can be long and
		// getScannedResults might be slow?
		for (final Scan scanner : scans) {
			try {
				final ResultScanner rs = hbaseOperations.getScannedResults(
						scanner,
						TABLE_PREFIX + secondaryIndex.getIndexStrategy().getId());

				if (rs != null) {
					results.add(rs);
				}
			}
			catch (final IOException e) {
				LOGGER.warn("Could not get the results from scanner " + e);
			}
		}

		final Collection<ByteArrayId> primaryIndexRowIds = new ArrayList<>();
		for (final ResultScanner resultsScan : results) {
			final Iterator<Result> it = resultsScan.iterator();
			while (it.hasNext()) {
				final Result result = it.next();
				// TODO Use breakpoints to find out what in the column/cells is
				// actually being checked in accumulo version and what to
				// correspondingly check here

				// for(result.getColumnCells(family, primaryIndexId)){
				//
				// }
				// if (entry.getKey().getColumnQualifier().toString().equals(
				// primaryIndexId.getString())) {
				// // found query match: keep track of primaryIndexRowId
				// primaryIndexRowIds.add(new ByteArrayId(
				// entry.getValue().get()));
				// }
			}
		}

		if (!primaryIndexRowIds.isEmpty()) {
			return new CloseableIteratorWrapper<ByteArrayId>(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							for (final ResultScanner resultsScan : results) {
								resultsScan.close();
							}
						}
					},
					primaryIndexRowIds.iterator());
		}
		return new CloseableIterator.Empty<ByteArrayId>();
	}

	@Override
	public void close() {
		for (final HBaseWriter writer : writerCache.values()) {
			writer.close();
		}
	}

	@Override
	public void flush() {
		close();
	}
}