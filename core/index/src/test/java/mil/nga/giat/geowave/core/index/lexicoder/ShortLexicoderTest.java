package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.UnsignedBytes;

public class ShortLexicoderTest extends
		AbstractLexicoderTest<Short>
{
	public ShortLexicoderTest() {
		super(
				Lexicoders.SHORT,
				Short.MIN_VALUE,
				Short.MAX_VALUE,
				new Short[] {
					(short) -10,
					Short.MIN_VALUE,
					(short) 2678,
					Short.MAX_VALUE,
					(short) 0
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
