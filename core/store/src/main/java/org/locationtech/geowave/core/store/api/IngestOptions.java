package org.locationtech.geowave.core.store.api;

import java.util.function.Function;
import java.util.function.Predicate;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.IngestOptionsBuilderImpl;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;

/**
 * When ingesting into a DataStore from a URL, this is a set of available
 * options that can be provided. Use the Builder to construct IngestOptions.
 *
 * @param <T>
 *            the type for entries that are being ingested
 */
public class IngestOptions<T>
{
	/**
	 * A Builder to create IngestOptions
	 *
	 * @param <T>
	 *            the type for entries that are being ingested
	 */
	public static interface Builder<T>
	{
		/**
		 * the ingest format plugin which does the actual parsing of the files
		 * and converting to GeoWave entries
		 *
		 * @param format
		 *            the format
		 * @return this builder
		 */
		Builder<T> format(
				LocalFileIngestPlugin<T> format );

		/**
		 * options that will be given to the ingest format plugin. Each plugin
		 * may use a different type of options.
		 *
		 * @param formatOptions
		 *            the options for the format plugin
		 * @return this builder
		 */
		Builder<T> formatOptions(
				IngestFormatOptions formatOptions );

		/**
		 * Number of threads to use for ingest
		 *
		 * @param threads
		 *            the number of threads
		 * @return this builder
		 */
		Builder<T> threads(
				int threads );

		/**
		 * Set a visibility string that will be applied to all data ingested/
		 *
		 * @param globalVisibility
		 *            the visibility to apply
		 * @return this builder
		 */
		Builder<T> visibility(
				String globalVisibility );

		/**
		 * Set an array of acceptable file extensions. If this is empty, all
		 * files will be accepted regardless of extension. Additionally each
		 * format plugin may only accept certain file extensions.
		 *
		 * @param fileExtensions
		 *            the array of acceptable file extensions
		 * @return this builder
		 */
		Builder<T> extensions(
				String[] fileExtensions );

		/**
		 * Add a new file extension to the array of acceptable file extensions
		 *
		 * @param fileExtension
		 *            the file extension to add
		 * @return this builder
		 */
		Builder<T> addExtension(
				String fileExtension );

		/**
		 * Filter data prior to being ingesting using a Predicate
		 *
		 * @param filter
		 *            the filter
		 * @return this builder
		 */
		Builder<T> filter(
				Predicate<T> filter );

		/**
		 * Transform the data prior to ingestion
		 *
		 * @param transform
		 *            the transform function
		 * @return this builder
		 */
		Builder<T> transform(
				Function<T, T> transform );

		/**
		 * register a callback to get notifications of the data and its
		 * insertion ID(s) within the indices after it has been ingested.
		 *
		 * @param callback
		 *            the callback
		 * @return this builder
		 */
		Builder<T> callback(
				IngestCallback<T> callback );

		/**
		 * Construct the IngestOptions with the provided values from this
		 * builder
		 *
		 * @return the IngestOptions
		 */
		IngestOptions<T> build();

		/**
		 * get a default implementation of this builder
		 *
		 * @return
		 */
		public static <T> Builder<T> newBuilder() {
			return new IngestOptionsBuilderImpl();
		}
	}

	/**
	 * An interface to get callbacks of ingest
	 *
	 * @param <T>
	 *            the type of data ingested
	 */
	public static interface IngestCallback<T>
	{
		void dataWritten(
				InsertionIds insertionIds,
				T data );
	}

	private final LocalFileIngestPlugin<T> format;
	private final IngestFormatOptions formatOptions;
	private final int threads;
	private final String globalVisibility;
	private final String[] fileExtensions;
	private final Predicate<T> filter;
	private final Function<T, T> transform;
	private final IngestCallback<T> callback;

	/**
	 * Use the Builder to construct instead of this constructor.
	 *
	 * @param format
	 *            the ingest format plugin
	 * @param formatOptions
	 *            options for the plugin
	 * @param threads
	 *            number of threads
	 * @param globalVisibility
	 *            visibility applied to all entries
	 * @param fileExtensions
	 *            an array of acceptable file extensions
	 * @param filter
	 *            a function to filter entries prior to ingest
	 * @param transform
	 *            a function to transform entries prior to ingest
	 * @param callback
	 *            a callback to get entries ingested and their insertion ID(s)
	 *            in GeoWave
	 */
	public IngestOptions(
			final LocalFileIngestPlugin<T> format,
			final IngestFormatOptions formatOptions,
			final int threads,
			final String globalVisibility,
			final String[] fileExtensions,
			final Predicate<T> filter,
			final Function<T, T> transform,
			final IngestCallback<T> callback ) {
		super();
		this.format = format;
		this.formatOptions = formatOptions;
		this.threads = threads;
		this.globalVisibility = globalVisibility;
		this.fileExtensions = fileExtensions;
		this.filter = filter;
		this.transform = transform;
		this.callback = callback;
	}

	public LocalFileIngestPlugin<T> getFormat() {
		return format;
	}

	public IngestFormatOptions getFormatOptions() {
		return formatOptions;
	}

	public int getThreads() {
		return threads;
	}

	public String getGlobalVisibility() {
		return globalVisibility;
	}

	public String[] getFileExtensions() {
		return fileExtensions;
	}

	public Predicate<T> getFilter() {
		return filter;
	}

	public Function<T, T> getTransform() {
		return transform;
	}

	public IngestCallback<T> getCallback() {
		return callback;
	}
}
