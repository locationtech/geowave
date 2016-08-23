package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.query.SecondaryIndexQueryFilterIterator;

public class AccumuloSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore<Mutation>
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloSecondaryIndexDataStore.class);
	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations ) {
		this(
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super();
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
	}

	@Override
	protected Writer<Mutation> getWriter(
			final SecondaryIndex<?> secondaryIndex ) {
		final String secondaryIndexName = secondaryIndex.getIndexStrategy().getId();
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		Writer<Mutation> writer = null;
		try {
			writer = accumuloOperations.createWriter(
					TABLE_PREFIX + secondaryIndexName,
					true,
					false,
					accumuloOptions.isEnableBlockCache(),
					secondaryIndex.getIndexStrategy().getNaturalSplits());
			writerCache.put(
					secondaryIndexName,
					writer);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Error creating writer",
					e);
		}
		return writer;
	}

	@Override
	protected Mutation buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName,
			final byte[] attributeValue,
			final byte[] visibility,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId ) {
		final Mutation m = new Mutation(
				secondaryIndexRowId);
		final ColumnVisibility columnVisibility = new ColumnVisibility(
				visibility);
		m.put(
				secondaryIndexId,
				attributeName,
				columnVisibility,
				attributeValue);
		m.put(
				secondaryIndexId,
				primaryIndexId,
				columnVisibility,
				primaryIndexRowId);
		return m;
	}

	@Override
	protected Mutation buildDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName ) {
		final Mutation m = new Mutation(
				secondaryIndexRowId);
		m.putDelete(
				secondaryIndexId,
				attributeName);
		return m;
	}

	@Override
	public CloseableIterator<ByteArrayId> query(
			final SecondaryIndex<?> secondaryIndex,
			final List<ByteArrayRange> ranges,
			final List<DistributableQueryFilter> constraints,
			final ByteArrayId primaryIndexId,
			final String... visibility ) {
		final Scanner scanner = getScanner(
				secondaryIndex.getIndexStrategy().getId(),
				visibility);
		if (scanner != null) {
			final Collection<ByteArrayId> primaryIndexRowIds = new ArrayList<>();
			scanner.addScanIterator(getScanIteratorSettings(
					constraints,
					primaryIndexId));
			final Collection<Range> scanRanges = getScanRanges(ranges);
			for (final Range range : scanRanges) {
				scanner.setRange(range);
				for (final Entry<Key, Value> entry : scanner) {
					if (entry.getKey().getColumnQualifier().toString().equals(
							primaryIndexId.getString())) {
						// found query match: keep track of primaryIndexRowId
						primaryIndexRowIds.add(new ByteArrayId(
								entry.getValue().get()));
					}
				}
			}
			return new CloseableIteratorWrapper<ByteArrayId>(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							scanner.close();
						}
					},
					primaryIndexRowIds.iterator());
		}
		return new CloseableIterator.Empty<ByteArrayId>();
	}

	private Scanner getScanner(
			final String secondaryIndexId,
			final String... visibility ) {
		Scanner scanner = null;
		try {
			scanner = accumuloOperations.createScanner(
					TABLE_PREFIX + secondaryIndexId,
					visibility);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Could not obtain batch scanner",
					e);
		}
		return scanner;
	}

	private Collection<Range> getScanRanges(
			final List<ByteArrayRange> ranges ) {
		if ((ranges == null) || ranges.isEmpty()) {
			return Collections.singleton(new Range());
		}
		final Collection<Range> scanRanges = new ArrayList<>();
		for (final ByteArrayRange range : ranges) {
			scanRanges.add(new Range(
					new Text(
							range.getStart().getBytes()),
					new Text(
							range.getEnd().getBytes())));
		}
		return scanRanges;
	}

	private IteratorSetting getScanIteratorSettings(
			final List<DistributableQueryFilter> distributableFilters,
			final ByteArrayId primaryIndexId ) {
		final IteratorSetting iteratorSettings = new IteratorSetting(
				SecondaryIndexQueryFilterIterator.ITERATOR_PRIORITY,
				SecondaryIndexQueryFilterIterator.ITERATOR_NAME,
				SecondaryIndexQueryFilterIterator.class);
		DistributableQueryFilter filter = getFilter(distributableFilters);
		if (filter != null) {
			iteratorSettings.addOption(
					SecondaryIndexQueryFilterIterator.FILTERS,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filter)));

		}
		iteratorSettings.addOption(
				SecondaryIndexQueryFilterIterator.PRIMARY_INDEX_ID,
				primaryIndexId.getString());
		return iteratorSettings;
	}
}