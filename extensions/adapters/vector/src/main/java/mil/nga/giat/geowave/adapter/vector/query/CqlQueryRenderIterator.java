package mil.nga.giat.geowave.adapter.vector.query;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.query.row.AbstractRowProvider;
import mil.nga.giat.geowave.adapter.vector.query.row.RowProviderSkippingIterator;
import mil.nga.giat.geowave.adapter.vector.wms.DistributableRenderer;
import mil.nga.giat.geowave.adapter.vector.wms.accumulo.RenderedMaster;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * This class can be used as an Accumulo Iterator and extends GeoWave's CQL
 * filter Iterator to additionally perform rendering within the tablet server. A
 * serialized renderer must be supplied in the options, and the renderer should
 * understand the style information to be able to iterate through SimpleFeatures
 * to render to images. Additionally, the serialized renderer can provide a row
 * skipping mechanism to perform smart decimation baased on rendered pixels. A
 * basic algorithm can be to maintain sorted set of rowId's per pixel and
 * eliminate pixels as they are rendered, then simply skip to the next row ID in
 * the set as data is rendered to the map. This iterator wraps a
 * RowProviderSkippingIterator which it delegates seek() method calls to in
 * order to perform this pixel-rendering informed seeking. Note the primary
 * reason these two Iterators are not separated and used independently within
 * the Accumulo Iterator stack is because the rendering must be tightly coupled
 * to the rows that are skipped.
 * 
 */
public class CqlQueryRenderIterator extends
		CqlQueryFilterIterator
{
	private final Logger LOGGER = Logger.getLogger(CqlQueryRenderIterator.class);
	public static final String CQL_QUERY_RENDER_ITERATOR_NAME = "GEOWAVE_CQL_RENDER_QUERY_FILTER";
	public static final int CQL_QUERY_RENDER_ITERATOR_PRIORITY = 10;
	public static final String RENDERER = "renderer";
	public static final String INDEX_STRATEGY = "index-strategy";
	private DistributableRenderer renderer;
	private boolean imageReturned = false;
	private Text startRowOfImage = null;
	private boolean reachedEnd = false;
	protected RowProviderSkippingIterator wrappedIterator;

	public CqlQueryRenderIterator() {
		super();
	}

	public CqlQueryRenderIterator(
			final SortedKeyValueIterator<Key, Value> source,
			final DistributableRenderer renderer ) {
		this.renderer = renderer;
		setSource(source);
	}

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		final boolean result = super.filter(
				currentRow,
				keys,
				values);

		try {
			if (!wrappedIterator.reseekAsNeeded()) {
				reachedEnd = true;
				return false;
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"unable to skip rows according to decimation options",
					e);
		}
		return result;
	}

	@Override
	protected boolean evaluateFeature(
			final Filter filter,
			final SimpleFeature feature,
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		final boolean filterResult = super.evaluateFeature(
				filter,
				feature,
				currentRow,
				keys,
				values);
		// always return false as we only care to stream back the resulting
		// image, but if its filtered in render it
		if (filterResult) {
			try {
				if (startRowOfImage == null) {
					startRowOfImage = currentRow;
				}
				renderer.render(feature);
			}
			catch (final Exception e) {
				LOGGER.warn(
						"unable to render feature",
						e);
			}
		}
		return false;
	}

	@Override
	public Key getTopKey() {
		if (hasTopFeature()) {
			return getTopFeatureKey();
		}
		else if (hasTopImage()) {
			return getTopImageKey();
		}
		return null;
	}

	@Override
	public Value getTopValue() {
		if (hasTopFeature()) {
			return getTopFeatureValue();
		}
		else if (hasTopImage()) {
			return getTopImageValue();
		}
		return null;
	}

	@Override
	public boolean hasTop() {
		// firstly iterate through all of the features
		final boolean hasTopFeature = hasTopFeature();
		if (hasTopFeature) {
			return true;
		}
		return hasTopImage();
	}

	@Override
	public void next()
			throws IOException {
		if (super.hasTop()) {
			super.next();
		}
		else {
			// there's only one master image, return it and finish
			imageReturned = true;
		}
	}

	@Override
	protected boolean defaultFilterResult() {
		return false;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		try {
			initClassLoader(getClass());
			final String rendererStr = options.get(RENDERER);
			final byte[] rendererBytes = ByteArrayUtils.byteArrayFromString(rendererStr);
			renderer = PersistenceUtils.fromBinary(
					rendererBytes,
					DistributableRenderer.class);

			final String indexStrategyStr = options.get(INDEX_STRATEGY);
			final byte[] indexStrategyBytes = ByteArrayUtils.byteArrayFromString(indexStrategyStr);
			final NumericIndexStrategy indexStrategy = PersistenceUtils.fromBinary(
					indexStrategyBytes,
					NumericIndexStrategy.class);
			final AbstractRowProvider rowProvider = renderer.newRowProvider(indexStrategy);
			wrappedIterator = new RowProviderSkippingIterator(
					source,
					rowProvider);
			super.init(
					source,
					options,
					env);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

	private Key getTopFeatureKey() {
		if (reachedEnd) {
			return null;
		}
		return super.getTopKey();
	}

	private Value getTopFeatureValue() {
		if (reachedEnd) {
			return null;
		}
		return super.getTopValue();
	}

	private boolean hasTopFeature() {
		if (reachedEnd) {
			return false;
		}
		return super.hasTop();
	}

	private Key getTopImageKey() {
		if (hasTopImage()) {
			return new Key(
					startRowOfImage);
		}
		return null;
	}

	private Value getTopImageValue() {
		if (hasTopImage()) {
			final RenderedMaster master = renderer.getResult();
			return new Value(
					PersistenceUtils.toBinary(master));
		}
		return null;
	}

	private boolean hasTopImage() {
		return !imageReturned && (startRowOfImage != null);
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		wrappedIterator.setSeekRange(
				range,
				columnFamilies,
				inclusive);
		super.seek(
				range,
				columnFamilies,
				inclusive);
	}
}
