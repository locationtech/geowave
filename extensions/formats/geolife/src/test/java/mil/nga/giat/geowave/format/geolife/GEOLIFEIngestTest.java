package mil.nga.giat.geowave.format.geolife;

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

public class GEOLIFEIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private GeoLifeIngestPlugin ingester;
	//private GDELTIngestPlugin ingesterExt;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new GeoLifeIngestPlugin();
		ingester.init(null);

		/*ingesterExt = new GeoLifeIngestPlugin( //TODO delete this late if det. don't need
				optionsProvider);
		ingesterExt.init(null);*/

		filePath = "20130401.export.CSV.zip"; // TODO this is probably not the right data to use
		expectedCount = 14056; // TODO get the right expected count
	}

	@Test
	public void testIngest()
			throws IOException {

		final File toIngest = new File(
				this.getClass().getClassLoader().getResource(
						filePath).getPath());

		assertTrue(GeoLifeUtils.validate(toIngest));
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

			if (isValidGeoLifeFeature(feature)) {
				featureCount++;
			}
		}
		features.close();

//		final CloseableIterator<GeoWaveData<SimpleFeature>> featuresExt = ingesterExt.toGeoWaveData(
//				toIngest,
//				indexIds,
//				"");

//		assertTrue((featuresExt != null) && featuresExt.hasNext());
//
//		int featureCountExt = 0;
//		while (featuresExt.hasNext()) {
//			final GeoWaveData<SimpleFeature> featureExt = featuresExt.next();
//
//			if (isValidGDELTFeatureExt(featureExt)) {
//				featureCountExt++;
//			}
//		}
//		featuresExt.close();

		final boolean readExpectedCount = (featureCount == expectedCount);
		if (!readExpectedCount) {
			System.out.println("Expected " + expectedCount + " features, ingested " + featureCount);
		}

		assertTrue(readExpectedCount);
	}
	
	/* PointBuilder things that should be set:
	 * geometry
	 * trackid
	 * pointinstance
	 * Timestamp
	 * Latitude
	 * Longitude
	 */
	/* PointBuilder things that can be null:
	 * elevation
	 */
	/* TrackBuilder things that should be set:
	 * geometry
	 * StartTimeStamp?
	 * EndTimeStamp?
	 * NumberPoints
	 * TrackId
	 */
	/* TrackBuilder things that can be null:
	 * Duration
	 */

	private boolean isValidGeoLifeFeature(	
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				"geometry") == null) || (feature.getValue().getAttribute(
				"trackid") == null) || (feature.getValue().getAttribute(
				"pointinstance") == null) || (feature.getValue().getAttribute(
				"Latitue") == null) || (feature.getValue().getAttribute(
				"Longitude") == null)) {
			return false;
		}
		return true;
	}

}
