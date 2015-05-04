package mil.nga.giat.geowave.types.gpx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.format.gpx.GpxIngestPlugin;
import mil.nga.giat.geowave.types.HelperClass;
import mil.nga.giat.geowave.types.ValidateObject;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GPXIngestPluginTest
{

	Map<String, ValidateObject<SimpleFeature>> expectedResults = new HashMap<String, ValidateObject<SimpleFeature>>();

	@Before
	public void setup() {

		expectedResults.put(
				"12345_1_Example_gpx",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Tags").toString().equals(
								"tag1 ||| tag2") && feature.getAttribute(
								"User").toString().equals(
								"Foo") && feature.getAttribute(
								"UserId").toString().equals(
								"12345") && feature.getAttribute(
								"TrackId").toString().equals(
								"12345") && feature.getAttribute(
								"NumberPoints").toString().equals(
								"7") && feature.getAttribute(
								"Duration").toString().equals(
								"251000") && feature.getAttribute("EndTimeStamp") != null && feature.getAttribute("StartTimeStamp") != null;
					}
				});
	}

	@Test
	public void test()
			throws IOException {
		Set<String> expectedSet = HelperClass.buildSet(expectedResults);

		GpxIngestPlugin pluggin = new GpxIngestPlugin();
		pluggin.init(new File(
				this.getClass().getClassLoader().getResource(
						"metadata.xml").getPath()).getParentFile());

		CloseableIterator<GeoWaveData<SimpleFeature>> consumer = pluggin.toGeoWaveData(
				new File(
						this.getClass().getClassLoader().getResource(
								"12345.xml").getPath()),
				new ByteArrayId(
						"123".getBytes(StringUtils.UTF8_CHAR_SET)),
				"");

		int totalCount = 0;
		while (consumer.hasNext()) {
			GeoWaveData<SimpleFeature> data = consumer.next();
			expectedSet.remove(data.getValue().getID());
			ValidateObject<SimpleFeature> tester = expectedResults.get(data.getValue().getID());
			if (tester != null) {
				assertTrue(
						data.getValue().toString(),
						tester.validate(data.getValue()));
			}
			totalCount++;
		}
		consumer.close();
		assertEquals(
				9,
				totalCount);
		// did everything get validated?
		if (expectedSet.size() > 0) {
			System.out.println("Failed matches:");
			System.out.println(expectedSet);
		}
		assertEquals(
				"All expected data set should be matched; zero unmatched data expected",
				0,
				expectedSet.size());
	}

}
