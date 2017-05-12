package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.awt.image.SampleModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.coverage.grid.GridCoverage;

/**
 * 
 * 
 * @param <T>
 */
public class RootMergeStrategy<T extends Persistable> implements
		Mergeable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RootMergeStrategy.class);
	// the purpose for these maps instead of a list of samplemodel and adapter
	// ID pairs is to allow for multiple adapters to share the same sample model
	protected Map<Integer, SampleModel> sampleModels = new HashMap<Integer, SampleModel>();
	public Map<ByteArrayId, Integer> adapterIdToSampleModelKey = new HashMap<ByteArrayId, Integer>();

	public Map<Integer, RasterTileMergeStrategy<T>> childMergeStrategies = new HashMap<Integer, RasterTileMergeStrategy<T>>();
	public Map<ByteArrayId, Integer> adapterIdToChildMergeStrategyKey = new HashMap<ByteArrayId, Integer>();

	protected RootMergeStrategy() {}

	public RootMergeStrategy(
			final ByteArrayId adapterId,
			final SampleModel sampleModel,
			final RasterTileMergeStrategy<T> mergeStrategy ) {
		sampleModels.put(
				0,
				sampleModel);
		adapterIdToSampleModelKey.put(
				adapterId,
				0);
		childMergeStrategies.put(
				0,
				mergeStrategy);
		adapterIdToChildMergeStrategyKey.put(
				adapterId,
				0);
	}

	public SampleModel getSampleModel(
			final ByteArrayId adapterId ) {
		synchronized (this) {
			final Integer sampleModelId = adapterIdToSampleModelKey.get(adapterId);
			if (sampleModelId != null) {
				return sampleModels.get(sampleModelId);
			}
			return null;
		}
	}

	public RasterTileMergeStrategy<T> getChildMergeStrategy(
			final ByteArrayId adapterId ) {
		synchronized (this) {
			final Integer childMergeStrategyId = adapterIdToChildMergeStrategyKey.get(adapterId);
			if (childMergeStrategyId != null) {
				return childMergeStrategies.get(childMergeStrategyId);
			}
			return null;
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		synchronized (this) {
			if ((merge != null) && (merge instanceof RootMergeStrategy)) {
				final RootMergeStrategy<T> other = (RootMergeStrategy) merge;
				mergeMaps(
						sampleModels,
						adapterIdToSampleModelKey,
						other.sampleModels,
						other.adapterIdToSampleModelKey);
				mergeMaps(
						childMergeStrategies,
						adapterIdToChildMergeStrategyKey,
						other.childMergeStrategies,
						other.adapterIdToChildMergeStrategyKey);
			}
		}
	}

	private static <T> void mergeMaps(
			final Map<Integer, T> thisValues,
			final Map<ByteArrayId, Integer> thisAdapterIdToValueKeys,
			final Map<Integer, T> otherValues,
			final Map<ByteArrayId, Integer> otherAdapterIdToValueKeys ) {
		// this was generalized to apply to both sample models and merge
		// strategies, comments refer to sample models but in general it is also
		// applied to merge strategies

		// first check for sample models that exist in 'other' that do
		// not exist in 'this'
		for (final Entry<Integer, T> sampleModelEntry : otherValues.entrySet()) {
			if (!thisValues.containsValue(sampleModelEntry.getValue())) {
				// we need to add this sample model
				final List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
				// find all adapter IDs associated with this sample
				// model
				for (final Entry<ByteArrayId, Integer> adapterIdEntry : otherAdapterIdToValueKeys.entrySet()) {
					if (adapterIdEntry.getValue().equals(
							sampleModelEntry.getKey())) {
						adapterIds.add(adapterIdEntry.getKey());
					}
				}
				if (!adapterIds.isEmpty()) {
					addValue(
							adapterIds,
							sampleModelEntry.getValue(),
							thisValues,
							thisAdapterIdToValueKeys);
				}
			}
		}
		// next check for adapter IDs that exist in 'other' that do not
		// exist in 'this'
		for (final Entry<ByteArrayId, Integer> adapterIdEntry : otherAdapterIdToValueKeys.entrySet()) {
			if (!thisAdapterIdToValueKeys.containsKey(adapterIdEntry.getKey())) {
				// find the sample model associated with the adapter ID
				// in 'other' and find what Integer it is with in 'this'
				final T sampleModel = otherValues.get(adapterIdEntry.getValue());
				if (sampleModel != null) {
					// because the previous step added any missing
					// sample models, it should be a fair assumption
					// that the sample model exists in 'this'
					for (final Entry<Integer, T> sampleModelEntry : thisValues.entrySet()) {
						if (sampleModel.equals(sampleModelEntry.getValue())) {
							// add the sample model key to the
							// adapterIdToSampleModelKey map
							thisAdapterIdToValueKeys.put(
									adapterIdEntry.getKey(),
									sampleModelEntry.getKey());
							break;
						}
					}
				}
			}
		}
	}

	private static synchronized <T> void addValue(
			final List<ByteArrayId> adapterIds,
			final T sampleModel,
			final Map<Integer, T> values,
			final Map<ByteArrayId, Integer> adapterIdToValueKeys ) {
		int nextId = 1;
		boolean idAvailable = false;
		while (!idAvailable) {
			boolean idMatched = false;
			for (final Integer id : values.keySet()) {
				if (nextId == id.intValue()) {
					idMatched = true;
					break;
				}
			}
			if (idMatched) {
				// try the next incremental ID
				nextId++;
			}
			else {
				// its not matched so we can use it
				idAvailable = true;
			}
		}
		values.put(
				nextId,
				sampleModel);
		for (final ByteArrayId adapterId : adapterIds) {
			adapterIdToValueKeys.put(
					adapterId,
					nextId);
		}
	}

	@SuppressFBWarnings(value = {
		"DLS_DEAD_LOCAL_STORE"
	}, justification = "Incorrect warning, sampleModelBinary used")
	@Override
	public byte[] toBinary() {
		int byteCount = 16;
		final List<byte[]> sampleModelBinaries = new ArrayList<byte[]>();
		final List<Integer> sampleModelKeys = new ArrayList<Integer>();
		int successfullySerializedModels = 0;
		int successfullySerializedModelAdapters = 0;
		final Set<Integer> successfullySerializedModelIds = new HashSet<Integer>();
		for (final Entry<Integer, SampleModel> entry : sampleModels.entrySet()) {
			final SampleModel sampleModel = entry.getValue();
			final SerializableState serializableSampleModel = SerializerFactory.getState(sampleModel);
			byte[] sampleModelBinary = new byte[0];
			try {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				final ObjectOutputStream oos = new ObjectOutputStream(
						baos);
				oos.writeObject(serializableSampleModel);
				oos.close();
				sampleModelBinary = baos.toByteArray();
				byteCount += sampleModelBinary.length;
				byteCount += 8;
				sampleModelBinaries.add(sampleModelBinary);
				sampleModelKeys.add(entry.getKey());
				successfullySerializedModels++;
				successfullySerializedModelIds.add(entry.getKey());
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to serialize sample model",
						e);
			}
		}

		for (final Entry<ByteArrayId, Integer> entry : adapterIdToSampleModelKey.entrySet()) {
			if (successfullySerializedModelIds.contains(entry.getValue())) {
				final byte[] keyBytes = entry.getKey().getBytes();
				byteCount += keyBytes.length + 8;
				successfullySerializedModelAdapters++;
			}
		}

		final List<byte[]> mergeStrategyBinaries = new ArrayList<byte[]>();
		final List<Integer> mergeStrategyKeys = new ArrayList<Integer>();
		int successfullySerializedMergeStrategies = 0;
		int successfullySerializedMergeAdapters = 0;
		final Set<Integer> successfullySerializedMergeIds = new HashSet<Integer>();
		for (final Entry<Integer, RasterTileMergeStrategy<T>> entry : childMergeStrategies.entrySet()) {
			final RasterTileMergeStrategy<T> mergeStrategy = entry.getValue();
			final byte[] mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);
			byteCount += mergeStrategyBinary.length;
			byteCount += 8;
			mergeStrategyBinaries.add(mergeStrategyBinary);
			mergeStrategyKeys.add(entry.getKey());
			successfullySerializedMergeStrategies++;
			successfullySerializedMergeIds.add(entry.getKey());
		}

		for (final Entry<ByteArrayId, Integer> entry : adapterIdToChildMergeStrategyKey.entrySet()) {
			if (successfullySerializedMergeIds.contains(entry.getValue())) {
				final byte[] keyBytes = entry.getKey().getBytes();
				byteCount += keyBytes.length + 8;
				successfullySerializedMergeAdapters++;
			}
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteCount);
		buf.putInt(successfullySerializedModels);
		for (int i = 0; i < successfullySerializedModels; i++) {
			final byte[] sampleModelBinary = sampleModelBinaries.get(i);
			buf.putInt(sampleModelBinary.length);
			buf.put(sampleModelBinary);
			buf.putInt(sampleModelKeys.get(i));
		}

		buf.putInt(successfullySerializedModelAdapters);
		for (final Entry<ByteArrayId, Integer> entry : adapterIdToSampleModelKey.entrySet()) {
			if (successfullySerializedModelIds.contains(entry.getValue())) {
				final byte[] keyBytes = entry.getKey().getBytes();
				buf.putInt(keyBytes.length);
				buf.put(keyBytes);
				buf.putInt(entry.getValue());
			}
		}
		buf.putInt(successfullySerializedMergeStrategies);
		for (int i = 0; i < successfullySerializedMergeStrategies; i++) {
			final byte[] mergeStrategyBinary = mergeStrategyBinaries.get(i);
			buf.putInt(mergeStrategyBinary.length);
			buf.put(mergeStrategyBinary);
			buf.putInt(mergeStrategyKeys.get(i));
		}

		buf.putInt(successfullySerializedMergeAdapters);
		for (final Entry<ByteArrayId, Integer> entry : adapterIdToChildMergeStrategyKey.entrySet()) {
			if (successfullySerializedModelIds.contains(entry.getValue())) {
				final byte[] keyBytes = entry.getKey().getBytes();
				buf.putInt(keyBytes.length);
				buf.put(keyBytes);
				buf.putInt(entry.getValue());
			}
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int sampleModelSize = buf.getInt();
		sampleModels = new HashMap<Integer, SampleModel>(
				sampleModelSize);
		for (int i = 0; i < sampleModelSize; i++) {
			final byte[] sampleModelBinary = new byte[buf.getInt()];
			if (sampleModelBinary.length > 0) {
				try {
					buf.get(sampleModelBinary);
					final ByteArrayInputStream bais = new ByteArrayInputStream(
							sampleModelBinary);
					final ObjectInputStream ois = new ObjectInputStream(
							bais);
					final Object o = ois.readObject();
					ois.close();
					final int sampleModelKey = buf.getInt();
					if ((o instanceof SerializableState)
							&& (((SerializableState) o).getObject() instanceof SampleModel)) {
						final SampleModel sampleModel = (SampleModel) ((SerializableState) o).getObject();
						sampleModels.put(
								sampleModelKey,
								sampleModel);
					}
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to deserialize sample model",
							e);
				}
			}
			else {
				LOGGER.warn("Sample model binary is empty, unable to deserialize");
			}
		}
		final int sampleModelAdapterIdSize = buf.getInt();
		adapterIdToSampleModelKey = new HashMap<ByteArrayId, Integer>(
				sampleModelAdapterIdSize);
		for (int i = 0; i < sampleModelAdapterIdSize; i++) {
			final int keyLength = buf.getInt();
			final byte[] keyBytes = new byte[keyLength];
			buf.get(keyBytes);
			adapterIdToSampleModelKey.put(
					new ByteArrayId(
							keyBytes),
					buf.getInt());
		}

		final int mergeStrategySize = buf.getInt();
		childMergeStrategies = new HashMap<Integer, RasterTileMergeStrategy<T>>(
				mergeStrategySize);
		for (int i = 0; i < mergeStrategySize; i++) {
			final byte[] mergeStrategyBinary = new byte[buf.getInt()];
			if (mergeStrategyBinary.length > 0) {
				try {
					buf.get(mergeStrategyBinary);
					final RasterTileMergeStrategy mergeStrategy = PersistenceUtils.fromBinary(
							mergeStrategyBinary,
							RasterTileMergeStrategy.class);
					final int mergeStrategyKey = buf.getInt();
					if (mergeStrategy != null) {
						childMergeStrategies.put(
								mergeStrategyKey,
								mergeStrategy);
					}
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to deserialize merge strategy",
							e);
				}
			}
			else {
				LOGGER.warn("Merge strategy binary is empty, unable to deserialize");
			}
		}
		final int mergeStrategyAdapterIdSize = buf.getInt();
		adapterIdToChildMergeStrategyKey = new HashMap<ByteArrayId, Integer>(
				mergeStrategyAdapterIdSize);
		for (int i = 0; i < mergeStrategyAdapterIdSize; i++) {
			final int keyLength = buf.getInt();
			final byte[] keyBytes = new byte[keyLength];
			buf.get(keyBytes);
			adapterIdToChildMergeStrategyKey.put(
					new ByteArrayId(
							keyBytes),
					buf.getInt());
		}
	}

	public void merge(
			final RasterTile<T> thisTile,
			final RasterTile<T> nextTile,
			final ByteArrayId dataAdapterId ) {
		final RasterTileMergeStrategy<T> childMergeStrategy = getChildMergeStrategy(dataAdapterId);

		if (childMergeStrategy != null) {
			childMergeStrategy.merge(
					thisTile,
					nextTile,
					getSampleModel(dataAdapterId));
		}
	}

	public T getMetadata(
			final GridCoverage tileGridCoverage,
			final Map originalCoverageProperties,
			final RasterDataAdapter dataAdapter ) {
		final RasterTileMergeStrategy<T> childMergeStrategy = getChildMergeStrategy(dataAdapter.getAdapterId());
		if (childMergeStrategy != null) {
			return childMergeStrategy.getMetadata(
					tileGridCoverage,
					dataAdapter);
		}
		return null;
	}
}
