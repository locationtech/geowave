package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Set;

public class NumericSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Number>
{

	private static final long serialVersionUID = 7098885526353176048L;
	public static final String INDEX_KEY = "2ND_IDX_NUMERIC";

	public NumericSecondaryIndexConfiguration(
			final String attribute ) {
		super(
				Number.class,
				attribute);
	}

	public NumericSecondaryIndexConfiguration(
			final Set<String> attributes ) {
		super(
				Number.class,
				attributes);
	}

	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
