/**
 * 
 */
package mil.nga.giat.geowave.adapter.vector.query.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.FilterToCQLTool;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsQuery;

import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author viggy
 * 
 */
public class HBaseCqlConstraintsQuery extends
		HBaseConstraintsQuery
{

	final static Logger LOGGER = LoggerFactory.getLogger(HBaseCqlConstraintsQuery.class);
	private final Filter cqlFilter;
	private final FeatureDataAdapter dataAdapter;

	public HBaseCqlConstraintsQuery(
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

	@Override
	protected List<org.apache.hadoop.hbase.filter.Filter> getDistributableFilter() {
		ArrayList<org.apache.hadoop.hbase.filter.Filter> dFilters = new ArrayList<org.apache.hadoop.hbase.filter.Filter>();

		try {
			dFilters.add(new CqlHBaseQueryFilter(
					FilterToCQLTool.toCQL(cqlFilter),
					PersistenceUtils.toBinary(index.getIndexModel()),
					PersistenceUtils.toBinary(dataAdapter)));
		}
		catch (CQLException e) {
			LOGGER.error("Error adding CqlHBaseQueryFilter" + e);

		}
		return super.getDistributableFilter();
	}

}
