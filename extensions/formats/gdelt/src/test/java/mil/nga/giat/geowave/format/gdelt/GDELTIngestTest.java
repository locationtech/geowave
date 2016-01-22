package mil.nga.giat.geowave.format.gdelt;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GDELTIngestTest
{
	private GDELTIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		ingester = new GDELTIngestPlugin();
		ingester.init(null);

		filePath = "20130401.export.CSV.zip";
		expectedCount = 14056;
	}

	@Test
	public void testIngest()
			throws IOException {

		final File toIngest = new File(
				this.getClass().getClassLoader().getResource(
						filePath).getPath());

		assertTrue(GDELTUtils.validate(toIngest));
		final Collection<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(new ByteArrayId(
				"123".getBytes(StringUtils.UTF8_CHAR_SET)));
		final CloseableIterator<GeoWaveData<SimpleFeature>> features = ingester.toGeoWaveData(
				toIngest,
				indexIds,
				"");

		assertTrue((features != null) && features.hasNext());

		int featureCount = 0;
		while (features.hasNext()) {
			final GeoWaveData<SimpleFeature> feature = features.next();

			if (isValidGDELTFeature(feature)) {
				featureCount++;
			}
		}
		features.close();

		final boolean readExpectedCount = (featureCount == expectedCount);
		if (!readExpectedCount) {
			System.out.println("Expected " + expectedCount + " features, ingested " + featureCount);
		}

		assertTrue(readExpectedCount);
	}

	private boolean isValidGDELTFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				GDELTUtils.GDELT_LATITUDE_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE) == null)) {
			return false;
		}
		return true;
	}
}
