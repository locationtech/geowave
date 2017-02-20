package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

/**
 * A set of convenience methods for common operations on Accumulo within
 * GeoWave, such as conversions between GeoWave objects and corresponding
 * Accumulo objects.
 *
 */
public class AccumuloUtils
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloUtils.class);
	private static final String ROW_MERGING_SUFFIX = "_COMBINER";
	private static final String ROW_MERGING_VISIBILITY_SUFFIX = "_VISIBILITY_COMBINER";

	public static Range byteArrayRangeToAccumuloRange(
			final ByteArrayRange byteArrayRange ) {
		final Text start = byteArrayRange.getStart().getBytes() == null ? null : new Text(
				byteArrayRange.getStart().getBytes());
		final Text end = byteArrayRange.getEnd().getBytes() == null ? null : new Text(
				byteArrayRange.getEnd().getBytes());
		if ((start != null) && (end != null) && (start.compareTo(end) > 0)) {
			return null;
		}
		return new Range(
				start,
				true,
				end == null ? null : Range.followingPrefix(end),
				false);
	}

	public static TreeSet<Range> byteArrayRangesToAccumuloRanges(
			final List<ByteArrayRange> byteArrayRanges ) {
		if (byteArrayRanges == null) {
			final TreeSet<Range> range = new TreeSet<Range>();
			range.add(new Range());
			return range;
		}
		final TreeSet<Range> accumuloRanges = new TreeSet<Range>();
		for (final ByteArrayRange byteArrayRange : byteArrayRanges) {
			final Range range = byteArrayRangeToAccumuloRange(byteArrayRange);
			if (range == null) {
				continue;
			}
			accumuloRanges.add(range);
		}
		if (accumuloRanges.isEmpty()) {
			// implies full table scan
			accumuloRanges.add(new Range());
		}
		return accumuloRanges;
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_"
				+ unqualifiedTableName;
	}

	/**
	 * Get Namespaces
	 *
	 * @param connector
	 */
	public static List<String> getNamespaces(
			final Connector connector ) {
		final List<String> namespaces = new ArrayList<String>();

		for (final String table : connector.tableOperations().list()) {
			final int idx = table.indexOf(AbstractGeoWavePersistence.METADATA_TABLE) - 1;
			if (idx > 0) {
				namespaces.add(table.substring(
						0,
						idx));
			}
		}
		return namespaces;
	}

	/**
	 * Get list of data adapters associated with the given namespace
	 *
	 * @param connector
	 * @param namespace
	 */
	public static List<DataAdapter<?>> getDataAdapters(
			final Connector connector,
			final String namespace ) {
		final List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();

		final AccumuloOptions options = new AccumuloOptions();
		final AdapterStore adapterStore = new AdapterStoreImpl(
				new AccumuloOperations(
						connector,
						namespace,
						options),
				options);

		try (final CloseableIterator<DataAdapter<?>> itr = adapterStore.getAdapters()) {

			while (itr.hasNext()) {
				adapters.add(itr.next());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}

		return adapters;
	}

	/**
	 * Get list of indices associated with the given namespace
	 *
	 * @param connector
	 * @param namespace
	 */
	public static List<Index<?, ?>> getIndices(
			final Connector connector,
			final String namespace ) {
		final List<Index<?, ?>> indices = new ArrayList<Index<?, ?>>();
		final AccumuloOptions options = new AccumuloOptions();
		final IndexStore indexStore = new IndexStoreImpl(
				new AccumuloOperations(
						connector,
						namespace,
						options),
				options);

		try (final CloseableIterator<Index<?, ?>> itr = indexStore.getIndices()) {

			while (itr.hasNext()) {
				indices.add(itr.next());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}

		return indices;
	}

	/**
	 * Set splits on a table based on a partition ID
	 *
	 * @param namespace
	 * @param index
	 * @param randomParitions
	 *            number of partition IDs
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByRandomPartitions(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int randomPartitions )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new AccumuloOperations(
				connector,
				namespace,
				new AccumuloOptions());
		final RoundRobinKeyIndexStrategy partitions = new RoundRobinKeyIndexStrategy(
				randomPartitions);

		operations.createTable(
				index.getId().getString(),
				true,
				true);
		for (final ByteArrayId p : partitions.getPartitionKeys()) {
			operations.insurePartition(
					p,
					index.getId().getString());
		}
	}

	/**
	 * Set splits on a table based on quantile distribution and fixed number of
	 * splits
	 *
	 * @param namespace
	 * @param index
	 * @param quantile
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByQuantile(
			final BaseDataStore dataStore,
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int quantile )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final long count = getEntries(
				dataStore,
				connector,
				namespace,
				index);

		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("Could not get iterator instance, getIterator returned null");
				throw new IOException(
						"Could not get iterator instance, getIterator returned null");
			}

			long ii = 0;
			final long splitInterval = (long) Math.ceil((double) count / (double) quantile);
			final SortedSet<Text> splits = new TreeSet<Text>();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				ii++;
				if (ii >= splitInterval) {
					ii = 0;
					splits.add(entry.getKey().getRow());
				}
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					index.getId().getString());
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Set splits on table based on equal interval distribution and fixed number
	 * of splits.
	 *
	 * @param namespace
	 * @param index
	 * @param numberSplits
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumSplits(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final int numSplits )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final SortedSet<Text> splits = new TreeSet<Text>();

		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("could not get iterator instance, getIterator returned null");
				throw new IOException(
						"could not get iterator instance, getIterator returned null");
			}

			final int numberSplits = numSplits - 1;
			BigInteger min = null;
			BigInteger max = null;

			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				final byte[] bytes = entry.getKey().getRow().getBytes();
				final BigInteger value = new BigInteger(
						bytes);
				if ((min == null) || (max == null)) {
					min = value;
					max = value;
				}
				min = min.min(value);
				max = max.max(value);
			}

			if ((min != null) && (max != null)) {
				final BigDecimal dMax = new BigDecimal(
						max);
				final BigDecimal dMin = new BigDecimal(
						min);
				BigDecimal delta = dMax.subtract(dMin);
				delta = delta.divideToIntegralValue(new BigDecimal(
						numSplits));

				for (int ii = 1; ii <= numberSplits; ii++) {
					final BigDecimal temp = delta.multiply(BigDecimal.valueOf(ii));
					final BigInteger value = min.add(temp.toBigInteger());

					final Text split = new Text(
							value.toByteArray());
					splits.add(split);
				}
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					StringUtils.stringFromBinary(index.getId().getBytes()));
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Set splits on table based on fixed number of rows per split.
	 *
	 * @param namespace
	 * @param index
	 * @param numberRows
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumRows(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final long numberRows )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		try (final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index)) {

			if (iterator == null) {
				LOGGER.error("Unable to get iterator instance, getIterator returned null");
				throw new IOException(
						"Unable to get iterator instance, getIterator returned null");
			}

			long ii = 0;
			final SortedSet<Text> splits = new TreeSet<Text>();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				ii++;
				if (ii >= numberRows) {
					ii = 0;
					splits.add(entry.getKey().getRow());
				}
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					StringUtils.stringFromBinary(index.getId().getBytes()));
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
	}

	/**
	 * Check if locality group is set.
	 *
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static boolean isLocalityGroupSet(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new AccumuloOperations(
				connector,
				namespace,
				new AccumuloOptions());
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		return operations.localityGroupExists(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Set locality group.
	 *
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setLocalityGroup(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new AccumuloOperations(
				connector,
				namespace,
				new AccumuloOptions());
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.addLocalityGroup(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * * Get number of entries per index.
	 *
	 * @param namespace
	 * @param index
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			final BaseDataStore dataStore,
			final Connector connector,
			final String namespace,
			final PrimaryIndex index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		final AccumuloOptions options = new AccumuloOptions();
		final AccumuloOperations operations = new AccumuloOperations(
				connector,
				namespace,
				options);
		final IndexStore indexStore = new IndexStoreImpl(
				operations,
				options);
		if (indexStore.indexExists(index.getId())) {
			final CloseableIterator<?> iterator = new AccumuloDataStore(
					operations,
					options).query(
					null,
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	public static void attachRowMergingIterators(
			final RowMergingDataAdapter<?, ?> adapter,
			final AccumuloOperations operations,
			final AccumuloOptions options,
			final String tableName )
			throws TableNotFoundException {
		final RowTransform rowTransform = adapter.getTransform();
		if (rowTransform != null) {
			final EnumSet<IteratorScope> visibilityCombinerScope = EnumSet.of(IteratorScope.scan);
			final OptionProvider optionProvider = new RowMergingAdapterOptionProvider(
					adapter);
			final IteratorConfig rowMergingCombinerConfig = new IteratorConfig(
					EnumSet.complementOf(visibilityCombinerScope),
					rowTransform.getBaseTransformPriority(),
					rowTransform.getTransformName() + ROW_MERGING_SUFFIX,
					RowMergingCombiner.class.getName(),
					optionProvider);
			final IteratorConfig rowMergingVisibilityCombinerConfig = new IteratorConfig(
					visibilityCombinerScope,
					rowTransform.getBaseTransformPriority() + 1,
					rowTransform.getTransformName() + ROW_MERGING_VISIBILITY_SUFFIX,
					RowMergingVisibilityCombiner.class.getName(),
					optionProvider);

			operations.attachIterators(
					tableName,
					options.isCreateTable(),
					true,
					options.isEnableBlockCache(),
					rowMergingCombinerConfig,
					rowMergingVisibilityCombinerConfig);
		}
	}

	private static CloseableIterator<Entry<Key, Value>> getIterator(
			final Connector connector,
			final String namespace,
			final PrimaryIndex index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		CloseableIterator<Entry<Key, Value>> iterator = null;
		final AccumuloOptions options = new AccumuloOptions();
		final AccumuloOperations operations = new AccumuloOperations(
				connector,
				namespace,
				new AccumuloOptions());
		final IndexStore indexStore = new IndexStoreImpl(
				operations,
				options);
		final AdapterStore adapterStore = new AdapterStoreImpl(
				operations,
				options);

		if (indexStore.indexExists(index.getId())) {
			final ScannerBase scanner = operations.createBatchScanner(index.getId().getString());
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));
			final IteratorSetting iteratorSettings = new IteratorSetting(
					10,
					"GEOWAVE_WHOLE_ROW_ITERATOR",
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);

			final Iterator<Entry<Key, Value>> it = new IteratorWrapper(
					adapterStore,
					index,
					scanner.iterator(),
					new DedupeFilter(),
					new AccumuloDataStore(
							operations,
							options));

			iterator = new CloseableIteratorWrapper<Entry<Key, Value>>(
					new ScannerClosableWrapper(
							scanner),
					it);
		}
		return iterator;
	}

	private static class IteratorWrapper implements
			Iterator<Entry<Key, Value>>
	{

		private final Iterator<Entry<Key, Value>> scannerIt;
		private final AdapterStore adapterStore;
		private final PrimaryIndex index;
		private final QueryFilter clientFilter;
		private Entry<Key, Value> nextValue;
		private final BaseDataStore dataStore;

		public IteratorWrapper(
				final AdapterStore adapterStore,
				final PrimaryIndex index,
				final Iterator<Entry<Key, Value>> scannerIt,
				final QueryFilter clientFilter,
				final BaseDataStore dataStore ) {
			this.adapterStore = adapterStore;
			this.index = index;
			this.scannerIt = scannerIt;
			this.clientFilter = clientFilter;
			this.dataStore = dataStore;
			findNext();
		}

		private void findNext() {
			while (scannerIt.hasNext()) {
				final Entry<Key, Value> row = scannerIt.next();
				final Object decodedValue = decodeRow(
						row,
						clientFilter,
						index);
				if (decodedValue != null) {
					nextValue = row;
					return;
				}
			}
			nextValue = null;
		}

		private Object decodeRow(
				final Entry<Key, Value> row,
				final QueryFilter clientFilter,
				final PrimaryIndex index ) {
			// TODO GEOWAVE-1018 - need to get this right
			return null;
			// return dataStore.decodeRow(
			// row.getKey(),
			// row.getValue(),
			// true,
			// // need to pass this, otherwise null value for rowId gets
			// // dereferenced later
			// new GeoWaveKeyImpl(
			// row.getKey().getRow().copyBytes()),
			// adapterStore,
			// clientFilter,
			// index);
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public Entry<Key, Value> next() {
			final Entry<Key, Value> previousNext = nextValue;
			findNext();
			return previousNext;
		}

		@Override
		public void remove() {}
	}

}