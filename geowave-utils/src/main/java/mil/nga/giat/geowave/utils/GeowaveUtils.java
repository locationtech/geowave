package mil.nga.giat.geowave.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.metadata.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.filter.DedupeFilter;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.query.Query;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Geowave utility class to perform convenience functionality:
 * 
 * Set splits on a table: Based on quantile distribution and fixed number of
 * splits. Based on equal interval distribution and fixed number of splits.
 * Based on fixed number of rows per split.
 * 
 * Get all geowave namespaces.
 * 
 * Set locality groups per column family (data adapter) or clear all locality
 * groups.
 * 
 * Get # of entries per data adapter in an index.
 * 
 * Get # of entries per index.
 * 
 * Get # of entries per namespace.
 * 
 * List adapters per namespace.
 * 
 * List indices per namespace.
 * 
 */

public class GeowaveUtils
{

	/**
	 * Get Namespaces
	 * 
	 * @param connector
	 */
	public static List<String> getNamespaces(
			final Connector connector ) {
		final List<String> namespaces = new ArrayList<String>();

		for (String table : connector.tableOperations().list()) {
			int idx = table.indexOf(AbstractAccumuloPersistence.METADATA_TABLE) - 1;
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
			Connector connector,
			String namespace ) {
		final List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();

		final AdapterStore adapterStore = new AccumuloAdapterStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		final Iterator<DataAdapter<?>> itr = adapterStore.getAdapters();

		while (itr.hasNext())
			adapters.add(itr.next());

		return adapters;
	}

	/**
	 * Get list of indices associated with the given namespace
	 * 
	 * @param connector
	 * @param namespace
	 */
	public static List<Index> getIndices(
			Connector connector,
			String namespace ) {
		final List<Index> indices = new ArrayList<Index>();

		final IndexStore indexStore = new AccumuloIndexStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		final Iterator<Index> itr = indexStore.getIndices();

		while (itr.hasNext())
			indices.add(itr.next());

		return indices;
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
			Connector connector,
			String namespace,
			Index index,
			int quantile )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		SortedSet<Text> splits = new TreeSet<Text>();

		CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		int numberSplits = quantile - 1;
		BigInteger min = null;
		BigInteger max = null;

		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			byte[] bytes = entry.getKey().getRow().getBytes();
			BigInteger value = new BigInteger(
					bytes);
			if (min == null || max == null) {
				min = value;
				max = value;
			}
			min = min.min(value);
			max = max.max(value);
		}

		BigDecimal dMax = new BigDecimal(
				max);
		BigDecimal dMin = new BigDecimal(
				min);
		BigDecimal delta = dMax.subtract(dMin);
		delta = delta.divideToIntegralValue(new BigDecimal(
				quantile));

		for (int ii = 1; ii <= numberSplits; ii++) {
			BigDecimal temp = delta.multiply(BigDecimal.valueOf(ii));
			BigInteger value = min.add(temp.toBigInteger());

			Text split = new Text(
					value.toByteArray());
			splits.add(split);
		}

		String tableName = AccumuloUtils.getQualifiedTableName(
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
			Connector connector,
			String namespace,
			Index index,
			int numberSplits )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		long count = getEntries(
				connector,
				namespace,
				index);

		CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		long ii = 0;
		long splitInterval = count / numberSplits;
		SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			ii++;
			if (ii >= splitInterval) {
				ii = 0;
				splits.add(entry.getKey().getRow());
			}
		}

		String tableName = AccumuloUtils.getQualifiedTableName(
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
			Connector connector,
			String namespace,
			Index index,
			long numberRows )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		long ii = 0;
		SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			Entry<Key, Value> entry = iterator.next();
			ii++;
			if (ii >= numberRows) {
				ii = 0;
				splits.add(entry.getKey().getRow());
			}
		}

		String tableName = AccumuloUtils.getQualifiedTableName(
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
			Connector connector,
			String namespace,
			Index index,
			DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
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
			Connector connector,
			String namespace,
			Index index,
			DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.addLocalityGroup(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Get number of entries for a data adapter in an index.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			Connector connector,
			String namespace,
			Index index,
			DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);
		if (indexStore.indexExists(index.getId()) && adapterStore.adapterExists(adapter.getAdapterId())) {
			List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
			adapterIds.add(adapter.getAdapterId());
			AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					adapterIds,
					index);
			CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * Get number of entries per index.
	 * 
	 * @param namespace
	 * @param index
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			Connector connector,
			String namespace,
			Index index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		if (indexStore.indexExists(index.getId())) {
			AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					index);
			CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * Get number of entries per namespace.
	 * 
	 * @param namespace
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			Connector connector,
			String namespace )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		AccumuloDataStore dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						connector,
						namespace));
		if (dataStore != null) {
			CloseableIterator<?> iterator = dataStore.query((Query) null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	private static CloseableIterator<Entry<Key, Value>> getIterator(
			Connector connector,
			String namespace,
			Index index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		CloseableIterator<Entry<Key, Value>> iterator = null;
		AccumuloOperations operations = new BasicAccumuloOperations(
				connector);
		AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		if (indexStore.indexExists(index.getId())) {
			String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
			final ScannerBase scanner = operations.createBatchScanner(tableName);
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));

			final IteratorSetting iteratorSettings = new IteratorSetting(
					10,
					"GEOWAVE_WHOLE_ROW_ITERATOR",
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);

			List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
			clientFilters.add(
					0,
					new DedupeFilter());

			Iterator<Entry<Key, Value>> it = new IteratorWrapper(
					adapterStore,
					index,
					scanner.iterator(),
					new FilterList<QueryFilter>(
							clientFilters));

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
		private AdapterStore adapterStore;
		private Index index;
		private QueryFilter clientFilter;
		private Entry<Key, Value> nextValue;

		public IteratorWrapper(
				final AdapterStore adapterStore,
				final Index index,
				final Iterator<Entry<Key, Value>> scannerIt,
				final QueryFilter clientFilter ) {
			this.adapterStore = adapterStore;
			this.index = index;
			this.scannerIt = scannerIt;
			this.clientFilter = clientFilter;
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
				final Index index ) {
			return AccumuloUtils.decodeRow(
					row.getKey(),
					row.getValue(),
					new AccumuloRowId(row.getKey()), //need to pass this, otherwise null value for rowId gets dereferenced later
					adapterStore,
					clientFilter,
					index);
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
