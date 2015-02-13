package mil.nga.giat.geowave.vector.query;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.query.TemporalRange;
import mil.nga.giat.geowave.vector.plugin.GeoWaveGTMemDataStore;
import mil.nga.giat.geowave.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.vector.utils.DateUtilities;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class TemporalRangeTest
{
	GeoWaveGTMemDataStore dataStore;
	SimpleFeatureType type;
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException {
		dataStore = new GeoWaveGTMemDataStore();
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");

		dataStore.createSchema(type);
	}

	@Test
	public void test()
			throws ParseException, IOException {
		Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		Calendar local = Calendar.getInstance(TimeZone.getTimeZone("EDT"));
		local.setTimeInMillis(gmt.getTimeInMillis());
		TemporalRange rGmt = new TemporalRange(
				gmt.getTime(),
				gmt.getTime());
		TemporalRange rLocal = new TemporalRange(
				local.getTime(),
				local.getTime());
		rGmt.fromBinary(rGmt.toBinary());
		assertEquals(
				gmt.getTime(),
				rGmt.getEndTime());
		assertEquals(
				rLocal.getEndTime(),
				rGmt.getEndTime());
		assertEquals(
				rLocal.getEndTime().getTime(),
				rGmt.getEndTime().getTime());

		Transaction transaction1 = new DefaultTransaction();

		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				Filter.EXCLUDE,
				transaction1);
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(77));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				DateUtilities.parseISO("2005-05-19T19:32:56-04:00"));
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						28.232)));

		FeatureTimeRangeStatistics stats = new FeatureTimeRangeStatistics(
				new ByteArrayId(
						"a"),
				"when");
		stats.entryIngested(
				null,
				newFeature);
		
		assertEquals(DateUtilities.parseISO("2005-05-19T23:32:56Z"), stats.asTemporalRange().getStartTime());
	}

}
