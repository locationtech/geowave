package org.locationtech.geowave.core.store.ingest;

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.IngestOptions.Builder;
import org.locationtech.geowave.core.store.api.IngestOptions.IngestCallback;

public class IngestOptionsBuilderImpl<T> implements
		Builder<T>
{

	private LocalFileIngestPlugin<T> format = null;
	private IngestFormatOptions formatOptions = null;
	private int threads = 1;
	private String globalVisibility = null;
	private String[] fileExtensions = new String[0];
	private Predicate<T> filter = null;
	private Function<T, T> transform = null;
	private IngestCallback<T> callback = null;

	@Override
	public Builder<T> format(
			final LocalFileIngestPlugin<T> format ) {
		this.format = format;
		return this;
	}

	@Override
	public Builder<T> formatOptions(
			final IngestFormatOptions formatOptions ) {
		this.formatOptions = formatOptions;
		return this;
	}

	@Override
	public Builder<T> threads(
			final int threads ) {
		this.threads = threads;
		return this;
	}

	@Override
	public Builder<T> visibility(
			final String globalVisibility ) {
		this.globalVisibility = globalVisibility;
		return this;
	}

	@Override
	public Builder<T> extensions(
			final String[] fileExtensions ) {
		this.fileExtensions = fileExtensions;
		return this;
	}

	@Override
	public Builder<T> addExtension(
			final String fileExtension ) {
		fileExtensions = ArrayUtils.add(
				fileExtensions,
				fileExtension);
		return this;
	}

	@Override
	public Builder<T> filter(
			final Predicate<T> filter ) {
		this.filter = filter;
		return this;
	}

	@Override
	public Builder<T> transform(
			final Function<T, T> transform ) {
		this.transform = transform;
		return this;
	}

	@Override
	public Builder<T> callback(
			final IngestCallback<T> callback ) {
		this.callback = callback;
		return this;
	}

	@Override
	public IngestOptions<T> build() {
		return new IngestOptions<>(
				format,
				formatOptions,
				threads,
				globalVisibility,
				fileExtensions,
				filter,
				transform,
				callback);
	}

}
