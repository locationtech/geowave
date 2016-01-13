package mil.nga.giat.geowave.format.gdelt;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

public class GDELTIngestTest
{
	private GDELTIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		ingester = new GDELTIngestPlugin();
		ingester.init(
				null);

		filePath = "20130401.export.CSV.zip";
		expectedCount = 27187;
	}

	@Test
	public void testIngest()
			throws IOException {

		final CloseableIterator<GeoWaveData<SimpleFeature>> features = ingester.toGeoWaveData(
				new File(
						this.getClass().getClassLoader().getResource(
								filePath).getPath()),
				new ByteArrayId(
						"123".getBytes(
								StringUtils.UTF8_CHAR_SET)),
				"");

		assertTrue(
				(features != null) && features.hasNext());

		int featureCount = 0;
		while (features.hasNext()) {
			final GeoWaveData<SimpleFeature> feature = features.next();

			if (isValidGDELTFeature(
					feature)) {
				featureCount++;
			}
		}
		features.close();

		final boolean readExpectedCount = (featureCount == expectedCount);
		if (!readExpectedCount) {
			System.out.println(
					"Expected " + expectedCount + " features, ingested " + featureCount);
		}

		assertTrue(
				readExpectedCount);
	}

	private boolean isValidGDELTFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE) == null)
				|| (feature.getValue().getAttribute(
						GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE) == null)
				|| (feature.getValue().getAttribute(
						GDELTUtils.GDELT_LATITUDE_ATTRIBUTE) == null)
				|| (feature.getValue().getAttribute(
						GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE) == null)
				|| (feature.getValue().getAttribute(
						GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE) == null)) {
			return false;
		}
		return true;
	}
}
