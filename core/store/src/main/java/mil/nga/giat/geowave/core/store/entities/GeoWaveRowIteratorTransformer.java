package mil.nga.giat.geowave.core.store.entities;

import java.util.Iterator;

import com.google.common.base.Function;

/**
 * Interface for a function that transforms an iterator of {@link GeoWaveRow}s
 * to another type. The interface transforms an iterator rather than an
 * individual row to allow iterators to merge rows before transforming them if
 * needed.
 *
 * @param <T>
 *            the type to transform each {@link GeoWaveRow} into
 */
public interface GeoWaveRowIteratorTransformer<T> extends
		Function<Iterator<GeoWaveRow>, Iterator<T>>
{
	public static GeoWaveRowIteratorTransformer<GeoWaveRow> NO_OP_TRANSFORMER = new GeoWaveRowIteratorTransformer<GeoWaveRow>() {

		@Override
		public Iterator<GeoWaveRow> apply(
				Iterator<GeoWaveRow> input ) {
			return input;
		}

	};
}
