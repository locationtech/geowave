package mil.nga.giat.geowave.datastore.accumulo;

import java.util.EnumSet;
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public class IteratorConfig
{
	private final EnumSet<IteratorScope> scopes;
	private final int iteratorPriority;
	private final String iteratorName;
	private final String iteratorClass;
	private final OptionProvider optionProvider;

	public IteratorConfig(
			final EnumSet<IteratorScope> scopes,
			final int iteratorPriority,
			final String iteratorName,
			final String iteratorClass,
			final OptionProvider optionProvider ) {
		this.scopes = scopes;
		this.iteratorPriority = iteratorPriority;
		this.iteratorName = iteratorName;
		this.iteratorClass = iteratorClass;
		this.optionProvider = optionProvider;
	}

	public EnumSet<IteratorScope> getScopes() {
		return scopes;
	}

	public int getIteratorPriority() {
		return iteratorPriority;
	}

	public String getIteratorName() {
		return iteratorName;
	}

	public String getIteratorClass() {
		return iteratorClass;
	}

	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		return optionProvider.getOptions(existingOptions);
	}

	public static interface OptionProvider
	{
		public Map<String, String> getOptions(
				Map<String, String> existingOptions );
	}
}
