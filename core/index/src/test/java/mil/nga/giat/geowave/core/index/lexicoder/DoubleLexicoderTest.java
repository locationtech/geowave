package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.SignedBytes;

public class DoubleLexicoderTest extends
		AbstractLexicoderTest<Double>
{
	public DoubleLexicoderTest() {
		super(
				Lexicoders.DOUBLE,
				Double.MIN_VALUE,
				Double.MAX_VALUE,
				new Double[] {
					-10d,
					Double.MIN_VALUE,
					11d,
					-14.2,
					14.2,
					-100.002,
					100.002,
					-11d,
					Double.MAX_VALUE,
					0d
				},
				SignedBytes.lexicographicalComparator());
	}
}
