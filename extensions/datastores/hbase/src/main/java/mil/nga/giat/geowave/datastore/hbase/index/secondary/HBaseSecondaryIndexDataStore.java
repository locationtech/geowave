package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class HBaseSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore<RowMutations>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseSecondaryIndexDataStore.class);
	private final BasicHBaseOperations hbaseOperations;
	private final HBaseOptions hbaseOptions;

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
	protected RowMutations buildDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName )
			throws Exception {
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
	public CloseableIterator<Object> scan(
			final ByteArrayId secondaryIndexId,
			final List<ByteArrayRange> scanRanges,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final String... visibility ) {
		// TODO implement ... mimick Accumulo approach
		return null;
	}

	// @Override
	// public CloseableIterator<ByteArrayId> query(
	// final SecondaryIndex<?> secondaryIndex,
	// final List<ByteArrayRange> ranges,
	// final List<DistributableQueryFilter> constraints,
	// final ByteArrayId primaryIndexId,
	// final String... visibility ) {
	//
	// final List<Scan> scans = new ArrayList<Scan>();
	//
	// for (final ByteArrayRange range : ranges) {
	// final Scan scanner = new Scan();
	// if (range.getStart() != null) {
	// scanner.setStartRow(range.getStart().getBytes());
	// if (!range.isSingleValue()) {
	// scanner.setStopRow(HBaseUtils.getNextPrefix(range.getEnd().getBytes()));
	// }
	// else {
	// scanner.setStopRow(HBaseUtils.getNextPrefix(range.getStart().getBytes()));
	// }
	// }
	//
	// scans.add(scanner);
	// }
	//
	// final List<ResultScanner> results = new ArrayList<ResultScanner>();
	//
	// // TODO Consider parallelization as list of scanners can be long and
	// // getScannedResults might be slow?
	// for (final Scan scanner : scans) {
	// try {
	// final ResultScanner rs = hbaseOperations.getScannedResults(
	// scanner,
	// TABLE_PREFIX +
	// StringUtils.stringFromBinary(secondaryIndex.getId().getBytes()),
	// visibility);
	//
	// if (rs != null) {
	// results.add(rs);
	// }
	// }
	// catch (final IOException e) {
	// LOGGER.warn("Could not get the results from scanner " + e);
	// }
	// }
	//
	// final Collection<ByteArrayId> primaryIndexRowIds = new ArrayList<>();
	// for (final ResultScanner resultsScan : results) {
	// final Iterator<Result> it = resultsScan.iterator();
	// while (it.hasNext()) {
	// final Result result = it.next();
	//
	// if (acceptRow(
	// result,
	// getFilter(constraints),
	// primaryIndexId)) {
	// for (final Cell cell : result.rawCells()) {
	// if (new ByteArrayId(
	// CellUtil.cloneQualifier(cell)).equals(primaryIndexId)) {
	// // found query match: keep track of
	// // primaryIndexRowId
	// primaryIndexRowIds.add(new ByteArrayId(
	// CellUtil.cloneValue(cell)));
	// }
	// }
	// }
	// }
	// }
	//
	// if (!primaryIndexRowIds.isEmpty()) {
	// return new CloseableIteratorWrapper<ByteArrayId>(
	// new Closeable() {
	// @Override
	// public void close()
	// throws IOException {
	// for (final ResultScanner resultsScan : results) {
	// resultsScan.close();
	// }
	// }
	// },
	// primaryIndexRowIds.iterator());
	// }
	// return new CloseableIterator.Empty<ByteArrayId>();
	// }

}