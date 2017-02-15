package mil.nga.giat.geowave.format.avro;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;

import mil.nga.giat.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class AVROIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private AvroIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new AvroIngestPlugin();
		ingester.init(null);

		filePath = "tornado_tracksbasicIT-export.avro";
		expectedCount = 474;

	}

	@Test
	public void testIngest()
			throws IOException {

		final File toIngest = new File(
				this.getClass().getClassLoader().getResource(
						filePath).getPath());

		assertTrue(validate(toIngest));
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

			if (isValidAVROFeature(feature)) {
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

	private boolean isValidAVROFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				"the_geom") == null) || (feature.getValue().getAttribute(
				"DATE") == null) || (feature.getValue().getAttribute(
				"OM") == null) || (feature.getValue().getAttribute(
				"ELAT") == null) || (feature.getValue().getAttribute(
				"ELON") == null) || (feature.getValue().getAttribute(
				"SLAT") == null) || (feature.getValue().getAttribute(
				"SLON") == null)) {
			return false;
		}
		return true;
	}

	private boolean validate(
			File file ) {
		try {
			DataFileReader.openReader(
					file,
					new SpecificDatumReader<AvroSimpleFeatureCollection>()).close();
			return true;
		}
		catch (final IOException e) {
			// Do nothing for now
		}

		return false;
	}
}
