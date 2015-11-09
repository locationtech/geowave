package mil.nga.giat.geowave.core.index;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ByteArrayRangeTest
{

	@Test
	public void test() {
		ByteArrayRange bar1 = new ByteArrayRange(
				new ByteArrayId(
						"232"),
				new ByteArrayId(
						"332"));
		ByteArrayRange bar2 = new ByteArrayRange(
				new ByteArrayId(
						"282"),
				new ByteArrayId(
						"300"));
		ByteArrayRange bar3 = new ByteArrayRange(
				new ByteArrayId(
						"272"),
				new ByteArrayId(
						"340"));
		ByteArrayRange bar4 = new ByteArrayRange(
				new ByteArrayId(
						"392"),
				new ByteArrayId(
						"410"));

		List<ByteArrayRange> l1 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar4,
						bar3,
						bar1,
						bar2));
		ByteArrayRange.mergeIntersections(
				l1,
				100);

		List<ByteArrayRange> l2 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar1,
						bar4,
						bar2,
						bar3));
		ByteArrayRange.mergeIntersections(
				l2,
				100);

		assertEquals(
				2,
				l1.size());

		assertEquals(
				l1,
				l2);

		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"232"),
						new ByteArrayId(
								"340")),
				l1.get(0));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"392"),
						new ByteArrayId(
								"410")),
				l1.get(1));

	}
}
