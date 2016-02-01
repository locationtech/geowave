package mil.nga.giat.geowave.adapter.raster;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;

public class RasterUtilsTest
{
	@Test
	public void testCreateDataAdapter() {
		final RasterDataAdapter adapter = RasterUtils.createDataAdapterTypeDouble(
				"test",
				3,
				256);
		Assert.assertNotNull(adapter);
		Assert.assertEquals(
				"test",
				adapter.getCoverageName());
		Assert.assertEquals(
				3,
				adapter.getSampleModel().getNumBands());
		Assert.assertEquals(
				256,
				adapter.getTileSize());
	}
}
