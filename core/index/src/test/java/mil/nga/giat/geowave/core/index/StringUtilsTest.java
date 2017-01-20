package mil.nga.giat.geowave.core.index;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StringUtilsTest
{
	@Test
	public void testFull() {
		String[] result = StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {
			"12",
			"34"
		}));
		assertEquals(
				2,
				result.length);
		assertEquals(
				"12",
				result[0]);
		assertEquals(
				"34",
				result[1]);
	}

	@Test
	public void testEmpty() {
		String[] result = StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {}));
		assertEquals(
				0,
				result.length);
	}
}
