package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

public class NumericSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Number>
{

	private static final long serialVersionUID = 7098885526353176048L;
	public static final String INDEX_KEY = "2ND_IDX_NUMERIC";

	public NumericSecondaryIndexConfiguration() {	
		super (Number.class, Collections.<String>emptySet());
	}
	
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

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
