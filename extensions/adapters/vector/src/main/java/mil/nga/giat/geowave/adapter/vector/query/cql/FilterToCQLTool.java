package mil.nga.giat.geowave.adapter.vector.query.cql;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.IllegalFilterException;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.filter.spatial.DWithinImpl;
import org.geotools.filter.spatial.IntersectsImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.filter.Filter;
import org.opengis.filter.MultiValuedFilter.MatchAction;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.DWithin;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

public class FilterToCQLTool
{
	private static Logger LOGGER = LoggerFactory.getLogger(FilterToCQLTool.class);

	/**
	 * Corrects any function issues.
	 * 
	 * @param filter
	 * @return
	 */
	public static String toCQL(
			Filter filter ) {

		FilterToECQLExtension toCQL = new FilterToECQLExtension();
		StringBuilder output = (StringBuilder) filter.accept(
				toCQL,
				new StringBuilder());
		return output.toString();
	}

	public static Filter toFilter(
			String expression )
			throws CQLException {
		return ECQL.toFilter(
				expression,
				new FilterFactoryImpl() {
					@Override
					public DWithin dwithin(
							Expression geometry1,
							Expression geometry2,
							double distance,
							String units,
							MatchAction matchAction ) {
						try {
							return matchAction == null ? new FixedDWithinImpl(
									geometry1,
									geometry2,
									units,
									distance) : new FixedDWithinImpl(
									geometry1,
									geometry2,
									units,
									distance,
									matchAction);
						}
						catch (IllegalFilterException | TransformException e) {
							LOGGER.warn(
									"Cannot convert DWithin Expression to work with WSG84",
									e);
						}
						final DWithinImpl impl = matchAction == null ? new DWithinImpl(
								geometry1,
								geometry2) : new DWithinImpl(
								geometry1,
								geometry2,
								matchAction);
						impl.setDistance(distance);
						impl.setUnits(units);
						return impl;
					}

					@Override
					public DWithin dwithin(
							Expression geometry1,
							Expression geometry2,
							double distance,
							String units ) {
						return dwithin(
								geometry1,
								geometry2,
								distance,
								units,
								(MatchAction) null);
					}
				});
	}

	public static final class FixedDWithinImpl extends
			IntersectsImpl implements
			DWithin
	{

		private double distance;
		private String units;

		public FixedDWithinImpl(
				Expression e1,
				Expression e2,
				String units,
				double distance )
				throws IllegalFilterException,
				TransformException {
			super(
					new LiteralExpressionImpl(
							mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils.buffer(
									getCRS(
											e1,
											e2),
									e1 instanceof PropertyName ? e2.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class) : e1.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class),
									units,
									distance).getLeft()),
					e1 instanceof PropertyName ? e1 : e2);
			this.units = units;
			this.distance = distance;
		}

		private static CoordinateReferenceSystem getCRS(
				Expression e1,
				Expression e2 ) {
			return GeoWaveGTDataStore.DEFAULT_CRS;
		}

		public FixedDWithinImpl(
				Expression e1,
				Expression e2,
				String units,
				double distance,
				MatchAction matchAction )
				throws IllegalFilterException,
				TransformException {
			super(
					new LiteralExpressionImpl(
							mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils.buffer(
									getCRS(
											e1,
											e2),
									e1.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class),
									units,
									distance).getLeft()),
					e2,
					matchAction);
			this.units = units;
			this.distance = distance;
		}

		@Override
		public double getDistance() {
			return distance;
		}

		@Override
		public String getDistanceUnits() {
			return units;
		}
	}

}
