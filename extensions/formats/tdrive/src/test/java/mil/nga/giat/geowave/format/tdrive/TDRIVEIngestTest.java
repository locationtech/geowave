package mil.nga.giat.geowave.format.tdrive;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import mil.nga.giat.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class TDRIVEIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private TdriveIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new TdriveIngestPlugin();
		ingester.init(null);

		filePath = "9879.txt";
		expectedCount = 232;
	}

	@Test
	public void testIngest()
			throws IOException {

		final File toIngest = new File(
				this.getClass().getClassLoader().getResource(
						filePath).getPath());

		assertTrue(TdriveUtils.validate(toIngest));
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

			if (isValidTDRIVEFeature(feature)) {
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

	private boolean isValidTDRIVEFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				"geometry") == null) || (feature.getValue().getAttribute(
				"taxiid") == null) || (feature.getValue().getAttribute(
				"pointinstance") == null) || (feature.getValue().getAttribute(
				"Timestamp") == null) || (feature.getValue().getAttribute(
				"Latitude") == null) || (feature.getValue().getAttribute(
				"Longitude") == null)) {
			return false;
		}
		return true;
	}

}
