package mil.nga.giat.geowave.adapter.vector.query;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.FilterToCQLTool;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.opengis.filter.Filter;

/**
 * This class extends the capabilities of a simple constraints query to support
 * GeoTools' CQL filters within the tablet servers.
 * 
 */
public class AccumuloCqlConstraintsQuery extends
		AccumuloConstraintsQuery
{
	private final Filter cqlFilter;
	private final FeatureDataAdapter dataAdapter;

	public AccumuloCqlConstraintsQuery(
			final Index index,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final String[] authorizations ) {
		super(
				Arrays.asList(dataAdapter.getAdapterId()),
				index,
				(MultiDimensionalNumericData) null,
				(List<QueryFilter>) new LinkedList<QueryFilter>(),
				authorizations);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final String[] authorizations ) {
		super(
				(List<ByteArrayId>) adapterIds,
				index,
				(MultiDimensionalNumericData) null,
				(List<QueryFilter>) new LinkedList<QueryFilter>(),
				authorizations);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final String[] authorizations ) {
		super(
				(List<ByteArrayId>) new LinkedList<ByteArrayId>(),
				index,
				(MultiDimensionalNumericData) constraints,
				(List<QueryFilter>) queryFilters,
				authorizations);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters,
				authorizations);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		if ((cqlFilter != null) && (dataAdapter != null)) {
			final IteratorSetting iteratorSettings = new IteratorSetting(
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_PRIORITY,
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_NAME,
					CqlQueryFilterIterator.class);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.CQL_FILTER,
					FilterToCQLTool.toCQL(cqlFilter));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.DATA_ADAPTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(dataAdapter)));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));

			final DistributableQueryFilter filterList = new DistributableFilterList(
					distributableFilters);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.GEOWAVE_FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filterList)));
			scanner.addScanIterator(iteratorSettings);
		}
		else {
			super.addScanIteratorSettings(scanner);
		}
	}
}
