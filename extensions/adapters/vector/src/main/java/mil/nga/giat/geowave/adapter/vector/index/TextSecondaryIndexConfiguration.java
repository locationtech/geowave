package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

public class TextSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<String>
{

	private static final long serialVersionUID = 8215331213775514560L;
	public static final String INDEX_KEY = "2ND_IDX_TEXT";

	public TextSecondaryIndexConfiguration() {
		super(
				String.class,
				Collections.<String> emptySet());
	}

	public TextSecondaryIndexConfiguration(
			final String attribute ) {
		super(
				String.class,
				attribute);
	}

	public TextSecondaryIndexConfiguration(
			final Set<String> attributes ) {
		super(
				String.class,
				attributes);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
