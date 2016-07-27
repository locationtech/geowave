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
import mil.nga.giat.geowave.core.index.StringUtils;
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

	@SuppressWarnings("unchecked")
	@Override
	protected Writer<Mutation> getWriter(
			final ByteArrayId secondaryIndexId ) {
		final String secondaryIndexName = secondaryIndexId.getString();
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		Writer<Mutation> writer = null;
		try {
			writer = accumuloOperations.createWriter(
					secondaryIndexName,
					true,
					false,
					accumuloOptions.isEnableBlockCache(),
					null);
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
	protected Mutation buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId,
			final byte[] attributeVisibility ) {
		final Mutation m = new Mutation(
				secondaryIndexRowId);
		final ColumnVisibility columnVisibility = new ColumnVisibility(
				attributeVisibility);
		m.put(
				ByteArrayUtils.combineArrays(
						adapterId,
						indexedAttributeFieldId),
				ByteArrayUtils.combineVariableLengthArrays(
						primaryIndexId,
						primaryIndexRowId),
				columnVisibility,
				EMPTY_VALUE);
		return m;
	}

	@Override
	protected Mutation buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility ) {
		final Mutation m = new Mutation(
				secondaryIndexRowId);
		final ColumnVisibility columnVisibility = new ColumnVisibility(
				fieldVisibility);
		m.put(
				ByteArrayUtils.combineArrays(
						adapterId,
						indexedAttributeFieldId),
				ByteArrayUtils.combineVariableLengthArrays(
						fieldId,
						dataId),
				columnVisibility,
				fieldValue);
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
	public CloseableIterator<Object> scan(
			ByteArrayId secondaryIndexId,
			final List<ByteArrayRange> scanRanges,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final String... visibility ) {
		final Scanner scanner = getScanner(
				StringUtils.stringFromBinary(secondaryIndexId.getBytes()),
				visibility);
		if (scanner != null) {
			scanner.fetchColumnFamily(new Text(
					ByteArrayUtils.combineArrays(
							adapterId.getBytes(),
							indexedAttributeFieldId.getBytes())));
			final Collection<Range> ranges = getScanRanges(scanRanges);
			for (final Range range : ranges) {
				scanner.setRange(range);
			}
			final Collection<Object> results = new ArrayList<>();
			for (final Entry<Key, Value> entry : scanner) {
				// TODO process entries to build results
				// ... requires notion of join strategy
				// ... which is not yet implemented
			}
			return new CloseableIteratorWrapper<Object>(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							scanner.close();
						}
					},
					results.iterator());
		}
		return new CloseableIterator.Empty<Object>();
	}

	private Scanner getScanner(
			final String secondaryIndexId,
			final String... visibility ) {
		Scanner scanner = null;
		try {
			scanner = accumuloOperations.createScanner(
					secondaryIndexId,
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