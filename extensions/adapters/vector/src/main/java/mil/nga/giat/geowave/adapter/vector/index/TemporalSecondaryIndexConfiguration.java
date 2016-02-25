package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

public class TemporalSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Date>
{

	private static final long serialVersionUID = 7158778533699252251L;
	public static final String INDEX_KEY = "2ND_IDX_TEMPORAL";

	public TemporalSecondaryIndexConfiguration() {
		super (Date.class, Collections.<String>emptySet());
	}
	public TemporalSecondaryIndexConfiguration(
			final String attribute ) {
		super(
				Date.class,
				attribute);
	}

	public TemporalSecondaryIndexConfiguration(
			final Set<String> attributes ) {
		super(
				Date.class,
				attributes);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
