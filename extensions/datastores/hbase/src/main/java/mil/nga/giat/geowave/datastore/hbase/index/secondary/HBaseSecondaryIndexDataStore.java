package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

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
			final SecondaryIndex<?> secondaryIndex ) {
		final String secondaryIndexName = secondaryIndex.getIndexStrategy().getId();
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		HBaseWriter writer = null;
		try {
			writer = hbaseOperations.createWriter(
					TABLE_PREFIX + secondaryIndexName,
					new String[] {
						StringUtils.stringFromBinary(secondaryIndex.getId().getBytes())
					},
					true,
					secondaryIndex.getIndexStrategy().getNaturalSplits());
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
	protected RowMutations buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName,
			final byte[] attributeValue,
			final byte[] visibility,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws Exception {
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

		p.addColumn(
				secondaryIndexId,
				primaryIndexId,
				primaryIndexRowId);

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
	public CloseableIterator<ByteArrayId> query(
			final SecondaryIndex<?> secondaryIndex,
			final List<ByteArrayRange> ranges,
			final List<DistributableQueryFilter> constraints,
			final ByteArrayId primaryIndexId,
			final String... visibility ) {

		final List<Scan> scans = new ArrayList<Scan>();

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
						TABLE_PREFIX + secondaryIndex.getIndexStrategy().getId(),
						visibility);

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

				if (acceptRow(
						result,
						getFilter(constraints),
						primaryIndexId)) {
					for (final Cell cell : result.rawCells()) {
						if (new ByteArrayId(
								CellUtil.cloneQualifier(cell)).equals(primaryIndexId)) {
							// found query match: keep track of
							// primaryIndexRowId
							primaryIndexRowIds.add(new ByteArrayId(
									CellUtil.cloneValue(cell)));
						}
					}
				}
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

	private boolean acceptRow(
			final Result result,
			final DistributableQueryFilter filter,
			final ByteArrayId primaryIndexId ) {
		if (filter != null) {
			for (final Cell cell : result.rawCells()) {
				if (!new ByteArrayId(
						CellUtil.cloneQualifier(cell)).equals(primaryIndexId)) {
					final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding = new IndexedPersistenceEncoding<ByteArrayId>(
							null, // not needed
							null, // not needed
							null, // not needed
							0, // not needed
							new PersistentDataset<ByteArrayId>(
									new PersistentValue<ByteArrayId>(
											new ByteArrayId(
													CellUtil.cloneQualifier(cell)),
											new ByteArrayId(
													CellUtil.cloneValue(cell)))),
							null);
					if (filter.accept(
							null,
							persistenceEncoding)) {
						return true;
					}
				}
			}

			return false;
		}
		// should not happen but if the filter is not passed in, it will accept
		// everything
		return true;
	}
}