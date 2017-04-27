package mil.nga.giat.geowave.core.cli;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VersionUtilsTest
{

	@Test
	public void testVersion() {
		final String version = null; // change this value when it gives a
										// version
		assertEquals(
				version, // change this value when it gives a version
				VersionUtils.getVersion());
	}
}
