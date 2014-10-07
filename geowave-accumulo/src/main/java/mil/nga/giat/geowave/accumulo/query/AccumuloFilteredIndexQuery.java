package mil.nga.giat.geowave.accumulo.query;

import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.accumulo.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

public abstract class AccumuloFilteredIndexQuery extends AccumuloQuery {
	 private List<QueryFilter> clientFilters;


private final static Logger LOGGER = Logger.getLogger(AccumuloFilteredIndexQuery.class);
	 
	public AccumuloFilteredIndexQuery(Index index, List<QueryFilter> clientFilters) {
		super(index);
		this.clientFilters = clientFilters;
	}

	public AccumuloFilteredIndexQuery(List<ByteArrayId> adapterIds, Index index,List<QueryFilter> clientFilters) {
		super(adapterIds, index);
		this.clientFilters = clientFilters;
	}

	public AccumuloFilteredIndexQuery(Index index) {
		super(index);
	}

	public AccumuloFilteredIndexQuery(List<ByteArrayId> adapterIds, Index index) {
		super(adapterIds, index);
	}

	
	protected List<QueryFilter> getClientFilters() {
		return clientFilters;
	}

	protected void setClientFilters(List<QueryFilter> clientFilters) {
		this.clientFilters = clientFilters;
	}

	protected abstract void addScanIteratorSettings(final ScannerBase scanner );
	
	@SuppressWarnings("rawtypes")
	public CloseableIterator<?> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		if (!accumuloOperations.tableExists(StringUtils.stringFromBinary(index.getId().getBytes()))) {
			LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
			return new CloseableIterator.Empty();
		}
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				limit);
		addScanIteratorSettings(scanner);
		Iterator it = new EntryIteratorWrapper(
				adapterStore,
				index,
				scanner.iterator(),
				new FilterList<QueryFilter>(
						clientFilters));
		if ((limit != null) && (limit >= 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIteratorWrapper(
				new ScannerClosableWrapper(
						scanner),
				it);
	}

}
