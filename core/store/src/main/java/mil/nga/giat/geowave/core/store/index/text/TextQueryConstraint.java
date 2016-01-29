package mil.nga.giat.geowave.core.store.index.text;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public abstract class TextQueryConstraint implements
		FilterableConstraints
{

	public abstract List<ByteArrayRange> getRange(
			int minNGramSize,
			int maxNGramSize );

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

}
