package mil.nga.giat.geowave.store.query;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

public class TemporalConstraintsTest
{

	@Test
	public void test() {
		TemporalConstraints constraints = new TemporalConstraints();
		constraints.add(new TemporalRange(
				new Date(
						1000),
				new Date(
						100002)));
		byte[] b = constraints.toBinary();

		TemporalConstraints constraintsDup = new TemporalConstraints();
		constraintsDup.fromBinary(b);

		assertEquals(
				constraints,
				constraintsDup);
	}

}
