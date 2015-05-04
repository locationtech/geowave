package mil.nga.giat.geowave.core.index.sfc.data;

import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;

import org.junit.Assert;
import org.junit.Test;

public class NumericValueTest
{

	private double VALUE = 50;
	private double DELTA = 1e-15;

	@Test
	public void testNumericValue() {

		NumericValue numericValue = new NumericValue(
				VALUE);

		Assert.assertEquals(
				VALUE,
				numericValue.getMin(),
				DELTA);
		Assert.assertEquals(
				VALUE,
				numericValue.getMax(),
				DELTA);
		Assert.assertEquals(
				VALUE,
				numericValue.getCentroid(),
				DELTA);
		Assert.assertFalse(numericValue.isRange());

	}
}
