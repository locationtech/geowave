package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Date;
import java.util.Set;

public class TemporalSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Date>
{

	private static final long serialVersionUID = 7158778533699252251L;
	public static final String INDEX_KEY = "2ND_IDX_TEMPORAL";

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

	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
