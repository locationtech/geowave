package mil.nga.giat.geowave.core.cli;

import org.junit.Assert;
import org.junit.Test;

public class GeoWaveMainTest
{
	@Test
	public void testMainHelp() {
		Assert.assertTrue(
				GeoWaveMain.run(
						new String[] {
							"--help"
		}) == 0);
	}
}
