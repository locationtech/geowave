package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;

public class PropertyManagementConverterTest
{

	@Test
	public void testConverter()
			throws Exception {
		PropertyManagement propMgmt = new PropertyManagement();
		PropertyManagementConverter conv = new PropertyManagementConverter(
				propMgmt);

		DBScanOptions opts = new DBScanOptions();
		opts.setGlobalBatchId("some-value");

		conv.readProperties(opts);

		Assert.assertEquals(
				"some-value",
				propMgmt.getProperty(GlobalParameters.Global.BATCH_ID));

	}
}
