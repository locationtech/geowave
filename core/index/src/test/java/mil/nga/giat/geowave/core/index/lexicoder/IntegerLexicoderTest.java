package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.UnsignedBytes;

public class IntegerLexicoderTest extends
		AbstractLexicoderTest<Integer>
{
	public IntegerLexicoderTest() {
		super(
				Lexicoders.INT,
				Integer.MIN_VALUE,
				Integer.MAX_VALUE,
				new Integer[] {
					-10,
					Integer.MIN_VALUE,
					2678,
					Integer.MAX_VALUE,
					0
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
