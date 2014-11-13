package mil.nga.giat.geowave.store.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import mil.nga.giat.geowave.index.sfc.data.NumericRange;

import org.junit.Test;

public class TemporalRangeTest {

	@Test
	public void test() {
		TemporalRange range = new TemporalRange(new Date(100), new Date(1000));
		assertFalse(range.isWithin(new Date(10)));
		assertFalse(range.isWithin(new Date(100000)));
		assertTrue(range.isWithin(new Date(800)));

		assertFalse(range.isWithin(new NumericRange(20, 99)));
		assertFalse(range.isWithin(new NumericRange(1001, 9900)));
		assertTrue(range.isWithin(new NumericRange(998, 9900)));
		assertTrue(range.isWithin(new NumericRange(20, 199)));
		assertTrue(range.isWithin(new NumericRange(150, 199)));
	}

}
