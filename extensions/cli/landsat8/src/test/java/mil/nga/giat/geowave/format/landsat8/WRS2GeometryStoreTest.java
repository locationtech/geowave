package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;

import org.junit.Test;

import static org.hamcrest.core.IsNull.notNullValue;

public class WRS2GeometryStoreTest
{
	@Test
	public void testGetGeometry()
			throws MalformedURLException,
			IOException {
		WRS2GeometryStore geometryStore = new WRS2GeometryStore(
				Tests.WORKSPACE_DIR);
		assertThat(
				geometryStore.getGeometry(
						1,
						1),
				notNullValue());
	}
}
