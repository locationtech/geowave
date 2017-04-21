package mil.nga.giat.geowave.format.gdelt;

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

public class GDELTIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private GDELTIngestPlugin ingester;
	private GDELTIngestPlugin ingesterExt;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new GDELTIngestPlugin();
		ingester.init(null);

		ingesterExt = new GDELTIngestPlugin(
				optionsProvider);
		ingesterExt.init(null);

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

		final CloseableIterator<GeoWaveData<SimpleFeature>> featuresExt = ingesterExt.toGeoWaveData(
				toIngest,
				indexIds,
				"");

		assertTrue((featuresExt != null) && featuresExt.hasNext());

		int featureCountExt = 0;
		while (featuresExt.hasNext()) {
			final GeoWaveData<SimpleFeature> featureExt = featuresExt.next();

			if (isValidGDELTFeatureExt(featureExt)) {
				featureCountExt++;
			}
		}
		featuresExt.close();

		final boolean readExpectedCount = (featureCount == expectedCount);
		if (!readExpectedCount) {
			System.out.println("Expected " + expectedCount + " features, ingested " + featureCount);
		}

		final boolean readExpectedCountExt = (featureCountExt == expectedCount);
		if (!readExpectedCount) {
			System.out.println("Expected " + expectedCount + " features, ingested " + featureCountExt);
		}

		assertTrue(readExpectedCount);
		assertTrue(readExpectedCountExt);
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

	private boolean isValidGDELTFeatureExt(
			final GeoWaveData<SimpleFeature> featureExt ) {
		if ((featureExt.getValue().getAttribute(
				GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE) == null) || (featureExt.getValue().getAttribute(
				GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE) == null) || (featureExt.getValue().getAttribute(
				GDELTUtils.GDELT_LATITUDE_ATTRIBUTE) == null) || (featureExt.getValue().getAttribute(
				GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE) == null) || (featureExt.getValue().getAttribute(
				GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE) == null) || (featureExt.getValue().getAttribute(
				GDELTUtils.AVG_TONE_ATTRIBUTE) == null)) {
			return false;
		}
		return true;
	}
}
