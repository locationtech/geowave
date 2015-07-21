package mil.nga.giat.geowave.core.ingest;

import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class CompoundIngestFormatOptionProvider implements
		IngestFormatOptionProvider
{

	private final List<IngestFormatOptionProvider> providers;

	public CompoundIngestFormatOptionProvider() {
		this(
				new LinkedList<IngestFormatOptionProvider>());
	}

	public CompoundIngestFormatOptionProvider(
			final List<IngestFormatOptionProvider> providers ) {
		this.providers = providers;
	}

	public CompoundIngestFormatOptionProvider add(
			IngestFormatOptionProvider provider ) {
		this.providers.add(provider);
		return this;
	}

	@Override
	public void applyOptions(
			Options allOptions ) {
		for (IngestFormatOptionProvider provider : providers) {
			provider.applyOptions(allOptions);
		}
	}

	@Override
	public void parseOptions(
			CommandLine commandLine ) {
		for (IngestFormatOptionProvider provider : providers) {
			provider.parseOptions(commandLine);
		}
	}
}
