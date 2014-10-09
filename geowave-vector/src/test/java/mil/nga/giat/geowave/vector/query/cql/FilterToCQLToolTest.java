package mil.nga.giat.geowave.vector.query.cql;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;


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
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");


	}
	
	@Test
	public void tesFid() {
		FilterFactoryImpl factory = new FilterFactoryImpl();
		Filter f = factory.createFidFilter("123-abc");
	 
		String ss = FilterToCQLTool.toCQL(f);
		assertTrue(ss.contains("'123-abc'"));
		
	}
	
	@Test
	public void test() {
		FilterFactoryImpl factory = new FilterFactoryImpl();
		Expression exp1 = factory.createAttributeExpression(type,"pid");
		Expression exp2 = factory.createLiteralExpression("a89dhd-123-abc");
		Filter f = factory.equal(exp1, exp2, false);
		String ss = FilterToCQLTool.toCQL(f);
		assertTrue(ss.contains("'a89dhd-123-abc'"));
		
	}

}
