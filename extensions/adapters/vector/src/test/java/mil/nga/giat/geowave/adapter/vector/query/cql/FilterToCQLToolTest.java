package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.expression.Expression;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FilterToCQLToolTest
{

	SimpleFeatureType type;

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException {
		type = DataUtilities.createType(
				"geostuff",
				"geom:Geometry:srid=4326,pop:java.lang.Long,pid:String");

	}

	@Test
	public void tesFid() {
		FilterFactoryImpl factory = new FilterFactoryImpl();
		Id f = factory.id(new FeatureIdImpl(
				"123-abc"));
		String ss = FilterToCQLTool.toCQL(f);
		assertTrue(ss.contains("'123-abc'"));

	}

	@Test
	public void test() {
		FilterFactoryImpl factory = new FilterFactoryImpl();
		Expression exp1 = factory.property("pid");
		Expression exp2 = factory.literal("a89dhd-123-abc");
		Filter f = factory.equal(
				exp1,
				exp2,
				false);
		String ss = FilterToCQLTool.toCQL(f);
		assertTrue(ss.contains("'a89dhd-123-abc'"));
	}

	@Test
	public void testDWithin()
			throws CQLException {
		Filter filter = CQL.toFilter("DWITHIN(geom, POINT(-122.7668 0.4979), 233.7, meters)");
		String gtFilterStr = FilterToCQLTool.toCQL(filter);
		assertEquals(
				"INTERSECTS(geom, POLYGON ((-122.76470055844142 0.4979, -122.76474089862228 0.4974904192702817, -122.76486036891433 0.4970965784983109, -122.76505437814123 0.4967336127640861, -122.76531547063722 0.4964154706372199, -122.76563361276409 0.4961543781412318, -122.76599657849832 0.49596036891432, -122.76639041927028 0.4958408986222733, -122.7668 0.4958005584414154, -122.76720958072973 0.4958408986222733, -122.76760342150169 0.49596036891432, -122.76796638723592 0.4961543781412318, -122.76828452936279 0.4964154706372199, -122.76854562185878 0.4967336127640861, -122.76873963108568 0.4970965784983109, -122.76885910137773 0.4974904192702817, -122.76889944155859 0.4979, -122.76885910137773 0.4983095807297183, -122.76873963108568 0.4987034215016891, -122.76854562185878 0.499066387235914, -122.76828452936279 0.4993845293627801, -122.76796638723592 0.4996456218587683, -122.76760342150169 0.49983963108568, -122.76720958072973 0.4999591013777267, -122.7668 0.4999994415585847, -122.76639041927028 0.4999591013777267, -122.76599657849832 0.49983963108568, -122.76563361276409 0.4996456218587683, -122.76531547063722 0.4993845293627801, -122.76505437814123 0.499066387235914, -122.76486036891433 0.4987034215016891, -122.76474089862228 0.4983095807297183, -122.76470055844142 0.4979)))",
				gtFilterStr);

		SimpleFeature newFeature = FeatureDataUtils.buildFeature(
				type,
				new Pair[] {
					Pair.of(
							"geom",
							new GeometryFactory().createPoint(new Coordinate(
									-122.76570055844142,
									0.4979))),
					Pair.of(
							"pop",
							Long.valueOf(100))

				});

		Filter gtFilter = ECQL.toFilter(gtFilterStr);
		assertTrue(gtFilter.evaluate(newFeature));

		SimpleFeature newFeatureToFail = FeatureDataUtils.buildFeature(
				type,
				new Pair[] {
					Pair.of(
							"geom",
							new GeometryFactory().createPoint(new Coordinate(
									-122.7690,
									0.4980))),
					Pair.of(
							"pop",
							Long.valueOf(100))

				});

		assertFalse(gtFilter.evaluate(newFeatureToFail));

	}

}
