package mil.nga.giat.geowave.adapter.vector.query;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.FilterToCQLTool;
import mil.nga.giat.geowave.adapter.vector.wms.DistributableRenderer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * This class extends the capabilities of a simple constraints query to perform
 * distributed rendering within tablet servers. It is able to serialize a
 * distributed renderer and return a set of features that contain the rendered
 * images ready for composition. It also supports CQL filters.
 * 
 */
public class DistributedRenderQuery extends
		AccumuloConstraintsQuery
{
	private final static Logger LOGGER = Logger.getLogger(DistributedRenderQuery.class);
	private final DistributableRenderer renderer;
	private final Filter cqlFilter;
	private final FeatureDataAdapter dataAdapter;

	public DistributedRenderQuery(
			final Index index,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final DistributableRenderer renderer,
			final String... authorizations ) {
		super(
				(List<ByteArrayId>) null, // adaptor ids
				index,
				(MultiDimensionalNumericData) null,
				(List<QueryFilter>) null,
				authorizations);
		this.renderer = renderer;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public DistributedRenderQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final DistributableRenderer renderer,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				(MultiDimensionalNumericData) null,
				(List<QueryFilter>) null,
				authorizations);
		this.renderer = renderer;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public DistributedRenderQuery(
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final DistributableRenderer renderer,
			final String... authorizations ) {
		super(
				(List<ByteArrayId>) null, // adaptor ids
				index,
				constraints,
				queryFilters,
				authorizations);
		this.renderer = renderer;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public DistributedRenderQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final DistributableRenderer renderer,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters,
				authorizations);
		this.renderer = renderer;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public List<CloseableIterator<SimpleFeature>> queryDistributedRender(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final boolean separateSubStrategies ) {
		final Map<NumericIndexStrategy, ScannerBase> scanners = getScanners(
				accumuloOperations,
				separateSubStrategies);
		final List<CloseableIterator<SimpleFeature>> iterators = new ArrayList<CloseableIterator<SimpleFeature>>();
		for (final Entry<NumericIndexStrategy, ScannerBase> entry : scanners.entrySet()) {
			final ScannerBase scanner = entry.getValue();
			addScanIteratorSettings(
					scanner,
					entry.getKey());
			iterators.add(new CloseableIteratorWrapper<SimpleFeature>(
					new Closeable() {

						@Override
						public void close()
								throws IOException {
							scanner.close();
						}
					},
					new RenderIteratorWrapper(
							scanner.iterator())));
		}
		return iterators;
	}

	protected Map<NumericIndexStrategy, ScannerBase> getScanners(
			final AccumuloOperations accumuloOperations,
			final boolean separateSubStrategies ) {
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		final Map<NumericIndexStrategy, ScannerBase> resultScanners = new HashMap<NumericIndexStrategy, ScannerBase>();
		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		if ((indexStrategy instanceof HierarchicalNumericIndexStrategy) && separateSubStrategies) {
			final SubStrategy[] subStrategies = ((HierarchicalNumericIndexStrategy) indexStrategy).getSubStrategies();
			for (final SubStrategy subStrategy : subStrategies) {
				resultScanners.put(
						subStrategy.getIndexStrategy(),
						getScanner(
								accumuloOperations,
								subStrategy.getPrefix(),
								tableName));
			}
		}
		else {
			resultScanners.put(
					indexStrategy,
					getScanner(
							accumuloOperations,
							null,
							tableName));
		}
		return resultScanners;
	}

	protected ScannerBase getScanner(
			final AccumuloOperations accumuloOperations,
			final byte[] prefix,
			final String tableName ) {
		BatchScanner scanner;
		try {
			scanner = accumuloOperations.createBatchScanner(
					tableName,
					this.getAdditionalAuthorizations());
			if ((prefix != null) && (prefix.length > 0)) {
				final Range r = Range.prefix(new Text(
						prefix));
				final List<Range> ranges = new ArrayList<Range>();
				ranges.add(r);
				scanner.setRanges(ranges);
			}
			else {
				final List<Range> ranges = new ArrayList<Range>();
				ranges.add(new Range());
				scanner.setRanges(ranges);
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.fetchColumnFamily(new Text(
						adapterId.getBytes()));
			}
		}
		return scanner;
	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner,
			final NumericIndexStrategy indexStrategy ) {
		if ((cqlFilter != null) && (dataAdapter != null)) {
			final IteratorSetting iteratorSettings = new IteratorSetting(
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_PRIORITY,
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_NAME,
					CqlQueryRenderIterator.class);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.CQL_FILTER,
					FilterToCQLTool.toCQL(cqlFilter));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.DATA_ADAPTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(dataAdapter)));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));
			iteratorSettings.addOption(
					CqlQueryRenderIterator.INDEX_STRATEGY,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(indexStrategy)));

			final DistributableQueryFilter filterList = new DistributableFilterList(
					distributableFilters);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.GEOWAVE_FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filterList)));
			iteratorSettings.addOption(
					CqlQueryRenderIterator.RENDERER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(renderer)));
			scanner.addScanIterator(iteratorSettings);
		}
		else {
			super.addScanIteratorSettings(scanner);
		}
	}

}
