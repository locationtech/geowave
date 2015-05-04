package mil.nga.giat.geowave.datastore.accumulo;

import java.util.EnumSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public class IteratorConfig
{
	private final IteratorSetting iteratorSettings;
	private final EnumSet<IteratorScope> scopes;

	public IteratorConfig(
			final IteratorSetting iteratorSettings,
			final EnumSet<IteratorScope> scopes ) {
		this.iteratorSettings = iteratorSettings;
		this.scopes = scopes;
	}

	public EnumSet<IteratorScope> getScopes() {
		return scopes;
	}

	public IteratorSetting getIteratorSettings() {
		return iteratorSettings;
	}

	public String mergeOption(
			final String optionKey,
			final String currentValue,
			final String nextValue ) {
		return nextValue;
	}
}
