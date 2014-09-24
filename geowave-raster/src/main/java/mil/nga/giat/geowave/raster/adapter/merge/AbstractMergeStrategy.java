package mil.nga.giat.geowave.raster.adapter.merge;

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

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.Persistable;

import org.apache.log4j.Logger;

/**
 *
 *
 * @param <T>
 */
abstract public class AbstractMergeStrategy<T extends Persistable> implements
		RasterTileMergeStrategy<T>
{
	private final static Logger LOGGER = Logger.getLogger(AbstractMergeStrategy.class);
	// the purpose for these maps instead of a list of samplemodel and adapter
	// ID pairs is to allow for multiple adapters to share the same sample model
	protected Map<Integer, SampleModel> sampleModels = new HashMap<Integer, SampleModel>();
	protected Map<ByteArrayId, Integer> adapterIdToSampleModelKey = new HashMap<ByteArrayId, Integer>();

	protected AbstractMergeStrategy() {}

	public AbstractMergeStrategy(
			final ByteArrayId adapterId,
			final SampleModel sampleModel ) {
		sampleModels.put(
				0,
				sampleModel);
		adapterIdToSampleModelKey.put(
				adapterId,
				0);
	}

	protected synchronized SampleModel getSampleModel(
			final ByteArrayId adapterId ) {
		final Integer sampleModelId = adapterIdToSampleModelKey.get(adapterId);
		if (sampleModelId != null) {
			return sampleModels.get(sampleModelId);
		}
		return null;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		synchronized (this) {
			if ((merge != null) && (merge instanceof AbstractMergeStrategy)) {
				final AbstractMergeStrategy<T> other = (AbstractMergeStrategy) merge;
				// first check for sample models that exist in 'other' that do
				// not
				// exist in 'this'
				for (final Entry<Integer, SampleModel> sampleModelEntry : other.sampleModels.entrySet()) {
					if (!sampleModels.containsValue(sampleModelEntry.getValue())) {
						// we need to add this sample model
						final List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
						// find all adapter IDs associated with this sample
						// model
						for (final Entry<ByteArrayId, Integer> adapterIdEntry : other.adapterIdToSampleModelKey.entrySet()) {
							if (adapterIdEntry.getValue().equals(
									sampleModelEntry.getKey())) {
								adapterIds.add(adapterIdEntry.getKey());
							}
						}
						if (!adapterIds.isEmpty()) {
							addSampleModel(
									adapterIds,
									sampleModelEntry.getValue());
						}
					}
				}
				// next check for adapter IDs that exist in 'other' that do not
				// exist in 'this'
				for (final Entry<ByteArrayId, Integer> adapterIdEntry : other.adapterIdToSampleModelKey.entrySet()) {
					if (!adapterIdToSampleModelKey.containsKey(adapterIdEntry.getKey())) {
						// find the sample model associated with the adapter ID
						// in 'other' and find what Integer it is with in 'this'
						final SampleModel sampleModel = other.sampleModels.get(adapterIdEntry.getValue());
						if (sampleModel != null) {
							// because the previous step added any missing
							// sample models, it should be a fair assumption
							// that the sample model exists in 'this'
							for (final Entry<Integer, SampleModel> sampleModelEntry : sampleModels.entrySet()) {
								if (sampleModel.equals(sampleModelEntry)) {
									// add the sample model key to the
									// adapterIdToSampleModelKey map
									adapterIdToSampleModelKey.put(
											adapterIdEntry.getKey(),
											sampleModelEntry.getKey());
									break;
								}
							}
						}
					}
				}
			}
		}
	}

	private synchronized void addSampleModel(
			final List<ByteArrayId> adapterIds,
			final SampleModel sampleModel ) {
		int nextId = 1;
		boolean idAvailable = false;
		while (!idAvailable) {
			boolean idMatched = false;
			for (final Integer id : sampleModels.keySet()) {
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
		sampleModels.put(
				nextId,
				sampleModel);
		for (final ByteArrayId adapterId : adapterIds) {
			adapterIdToSampleModelKey.put(
					adapterId,
					nextId);
		}
	}

	@Override
	public byte[] toBinary() {
		int byteCount = 8;
		final List<byte[]> sampleModelBinaries = new ArrayList<byte[]>();
		final List<Integer> sampleModelKeys = new ArrayList<Integer>();
		int successfullySerializedModels = 0;
		int successfullySerializedAdapters = 0;
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
				successfullySerializedAdapters++;
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

		buf.putInt(successfullySerializedAdapters);
		for (final Entry<ByteArrayId, Integer> entry : adapterIdToSampleModelKey.entrySet()) {
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
					final int sampleModelKey = buf.getInt();
					if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof SampleModel)) {
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
		final int adapterIdSize = buf.getInt();
		adapterIdToSampleModelKey = new HashMap<ByteArrayId, Integer>(
				adapterIdSize);
		for (int i = 0; i < adapterIdSize; i++) {
			final int keyLength = buf.getInt();
			final byte[] keyBytes = new byte[keyLength];
			buf.get(keyBytes);
			adapterIdToSampleModelKey.put(
					new ByteArrayId(
							keyBytes),
					buf.getInt());
		}

	}
}
