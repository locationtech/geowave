package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class MultiOptionProvider implements
		IngestFormatOptionProvider
{
	private final IngestFormatOptionProvider[] optionProviders;

	public MultiOptionProvider(
			final IngestFormatOptionProvider[] optionProviders ) {
		this.optionProviders = optionProviders;
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		for (final IngestFormatOptionProvider p : optionProviders) {
			p.applyOptions(allOptions);
		}
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		for (final IngestFormatOptionProvider p : optionProviders) {
			p.parseOptions(commandLine);
		}
	}

}
