package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

public class NumericSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Number>
{

	private static final long serialVersionUID = 2276091974744030581L;
	public static final String INDEX_KEY = "2ND_IDX_NUMERIC";

	public NumericSecondaryIndexConfiguration() {
		super(
				Number.class,
				Collections.<String> emptySet(),
				SecondaryIndexType.JOIN);
	}

	public NumericSecondaryIndexConfiguration(
			final String attribute,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Number.class,
				attribute,
				secondaryIndexType);
	}

	public NumericSecondaryIndexConfiguration(
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Number.class,
				attributes,
				secondaryIndexType);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
