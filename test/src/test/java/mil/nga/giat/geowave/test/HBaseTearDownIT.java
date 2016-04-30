package mil.nga.giat.geowave.test;

import org.junit.Test;

public class HBaseTearDownIT
{
	@Test
	public void tearDownHbase() {
		HBaseStoreTestEnvironment.getInstance().tearDown();
	}
}
