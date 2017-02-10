package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.AllOf.allOf;

public class SceneFeatureIteratorTest
{
	private Matcher<SimpleFeature> hasProperties() {
		return new BaseMatcher<SimpleFeature>() {
			@Override
			public boolean matches(
					Object item ) {
				SimpleFeature feature = (SimpleFeature) item;

				return feature.getProperty("entityId") != null && feature.getProperty("acquisitionDate") != null
						&& feature.getProperty("cloudCover") != null && feature.getProperty("processingLevel") != null
						&& feature.getProperty("path") != null && feature.getProperty("row") != null
						&& feature.getProperty("sceneDownloadUrl") != null;
			}

			@Override
			public void describeTo(
					Description description ) {
				description
						.appendText("feature should have properties {entityId, acquisitionDate, cloudCover, processingLevel, path, row, sceneDownloadUrl}");
			}
		};
	}

	private Matcher<SimpleFeature> inBounds(
			BoundingBox bounds ) {
		return new BaseMatcher<SimpleFeature>() {
			@Override
			public boolean matches(
					Object item ) {
				SimpleFeature feature = (SimpleFeature) item;
				return feature.getBounds().intersects(
						bounds);
			}

			@Override
			public void describeTo(
					Description description ) {
				description.appendText("feature should be in bounds " + bounds);
			}
		};
	}

	@Test
	public void testIterate()
			throws IOException,
			CQLException {
		boolean onlyScenesSinceLastRun = false;
		boolean useCachedScenes = true;
		boolean nBestScenesByPathRow = false;
		int nBestScenes = 1;
		Filter cqlFilter = CQL.toFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA'");
		String workspaceDir = Tests.WORKSPACE_DIR;

		List<SimpleFeature> features = new ArrayList<>();
		try (SceneFeatureIterator iterator = new SceneFeatureIterator(
				onlyScenesSinceLastRun,
				useCachedScenes,
				nBestScenesByPathRow,
				nBestScenes,
				cqlFilter,
				workspaceDir)) {
			while (iterator.hasNext()) {
				features.add(iterator.next());
			}
		}

		assertEquals(
				features.size(),
				1);
		assertThat(
				features,
				everyItem(allOf(
						hasProperties(),
						inBounds(new Envelope2D(
								new DirectPosition2D(
										-76.6,
										42.34),
								new DirectPosition2D(
										-76.4,
										42.54))))));
	}

}
