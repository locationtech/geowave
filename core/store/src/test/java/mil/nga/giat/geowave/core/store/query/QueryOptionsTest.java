package mil.nga.giat.geowave.core.store.query;

import java.util.Arrays;

import org.junit.Test;

public class QueryOptionsTest
{

	@Test
	public void test() {
		QueryOptions ops = new QueryOptions();
		ops.fromBinary(ops.toBinary());

		ops.setFieldIds(Arrays.asList(
				"123|",
				"|abc"));
		ops.fromBinary(ops.toBinary());
		ops.getFieldIds().contains(
				"123|");
		ops.getFieldIds().contains(
				"|abc");
	}
}
