package mil.nga.giat.geowave.vector.query;

import java.util.List;

import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.geotools.filter.text.ecql.ECQL;
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
			final FeatureDataAdapter dataAdapter ) {
		super(
				index);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter ) {
		super(
				adapterIds,
				index);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter ) {
		super(
				index,
				constraints,
				queryFilters);
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public AccumuloCqlConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters);
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
					ECQL.toCQL(cqlFilter));
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
