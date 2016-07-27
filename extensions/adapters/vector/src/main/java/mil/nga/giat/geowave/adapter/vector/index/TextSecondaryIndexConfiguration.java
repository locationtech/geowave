package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

public class TextSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<String>
{

	private static final long serialVersionUID = -6056277252664164096L;
	public static final String INDEX_KEY = "2ND_IDX_TEXT";

	public TextSecondaryIndexConfiguration() {
		super(
				String.class,
				Collections.<String> emptySet(),
				SecondaryIndexType.JOIN);
	}

	public TextSecondaryIndexConfiguration(
			final String attribute,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				String.class,
				attribute,
				secondaryIndexType);
	}

	public TextSecondaryIndexConfiguration(
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				String.class,
				attributes,
				secondaryIndexType);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
