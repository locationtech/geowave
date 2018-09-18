package org.locationtech.geowave.core.store.api;

import java.util.function.Function;
import java.util.function.Predicate;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;

public class IngestOptions<T>
{
	public static interface Builder<T>
	{
		Builder<T> format(
				LocalFileIngestPlugin<T> format );

		Builder<T> formatOptions(
				IngestFormatOptions formatOptions );

		Builder<T> threads(
				int threads );

		Builder<T> visibility(
				String globalVisiblity );

		Builder<T> extensions(
				String[] fileExtensions );

		Builder<T> addExtension(
				String fileExtension );

		Builder<T> filter(
				Predicate<T> filter );

		Builder<T> transform(
				Function<T, T> transform );

		Builder<T> callback(
				IngestCallback<T> callback );

		IngestOptions<T> build();

		public static <T> Builder<T> newBuilder() {
			return null;
		}
	}

	public static interface IngestCallback<T>
	{
		void dataWritten(
				InsertionIds insertionIds,
				T data );
	}

}
