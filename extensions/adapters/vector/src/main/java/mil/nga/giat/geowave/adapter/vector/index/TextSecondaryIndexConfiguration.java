package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

import org.codehaus.jackson.annotate.JsonIgnore;

public class TextSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<String>
{

	private static final long serialVersionUID = -561162921233891848L;
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

	public TextSecondaryIndexConfiguration(
			final Class<String> clazz,
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldIds ) {
		super(
				clazz,
				attributes,
				secondaryIndexType,
				fieldIds);
	}

	public TextSecondaryIndexConfiguration(
			Class<String> clazz,
			Set<String> attributes,
			SecondaryIndexType secondaryIndexType ) {
		super(
				clazz,
				attributes,
				secondaryIndexType);
		// TODO Auto-generated constructor stub
	}

	public TextSecondaryIndexConfiguration(
			Class<String> clazz,
			String attribute,
			SecondaryIndexType secondaryIndexType ) {
		super(
				clazz,
				attribute,
				secondaryIndexType);
		// TODO Auto-generated constructor stub
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
