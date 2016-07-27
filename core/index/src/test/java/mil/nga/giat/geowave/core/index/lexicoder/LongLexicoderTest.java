package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.UnsignedBytes;

public class LongLexicoderTest extends
		AbstractLexicoderTest<Long>
{
	public LongLexicoderTest() {
		super(
				Lexicoders.LONG,
				Long.MIN_VALUE,
				Long.MAX_VALUE,
				new Long[] {
					-10l,
					Long.MIN_VALUE,
					2678l,
					Long.MAX_VALUE,
					0l
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
