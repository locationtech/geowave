package org.locationtech.geowave.core.geotime.store.query;

import java.net.MalformedURLException;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialTemporalConstraintsBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryConstraintsFactory;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactoryImpl;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorQueryConstraintsFactoryImpl extends
		QueryConstraintsFactoryImpl implements
		VectorQueryConstraintsFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(OptimalCQLQuery.class);

	public static final VectorQueryConstraintsFactoryImpl SINGLETON_INSTANCE = new VectorQueryConstraintsFactoryImpl();

	public SpatialTemporalConstraintsBuilder spatialTemporalConstraints() {
		return new SpatialTemporalConstraintsBuilderImpl();
	}

	// these cql expressions should always attempt to use
	// CQLQuery.createOptimalQuery() which requires adapter and index
	public QueryConstraints cqlConstraints(
			final String cqlExpression ) {

		try {
			GeometryUtils.initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.error(
					"Unable to initialize GeoTools class loader",
					e);
		}
		try {
			final Filter cqlFilter = ECQL.toFilter(cqlExpression);
			return new OptimalCQLQuery(
					cqlFilter);
		}
		catch (final CQLException e) {
			LOGGER.error(
					"Unable to parse CQL expresion",
					e);
		}
		return null;
	}

	public QueryConstraints filterConstraints(
			final Filter filter ) {
		return new OptimalCQLQuery(
				filter);
	}
}
