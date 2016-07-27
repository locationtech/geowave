package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

public class TemporalSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Date>
{

	private static final long serialVersionUID = -7046022550752862268L;
	public static final String INDEX_KEY = "2ND_IDX_TEMPORAL";

	public TemporalSecondaryIndexConfiguration() {
		super(
				Date.class,
				Collections.<String> emptySet(),
				SecondaryIndexType.JOIN);
	}

	public TemporalSecondaryIndexConfiguration(
			final String attribute,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Date.class,
				attribute,
				secondaryIndexType);
	}

	public TemporalSecondaryIndexConfiguration(
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Date.class,
				attributes,
				secondaryIndexType);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
