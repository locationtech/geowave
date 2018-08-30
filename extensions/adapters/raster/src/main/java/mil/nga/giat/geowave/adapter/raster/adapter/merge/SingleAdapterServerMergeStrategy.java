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
import java.util.Set;
import java.util.Map.Entry;

import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;

import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

public class SingleAdapterServerMergeStrategy<T extends Persistable> implements
		ServerMergeStrategy,
		Persistable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SingleAdapterServerMergeStrategy.class);
	// the purpose for these maps instead of a list of samplemodel and adapter
	// ID pairs is to allow for multiple adapters to share the same sample model
	protected short internalAdapterId;
	protected SampleModel sampleModel;
	protected RasterTileMergeStrategy<T> mergeStrategy;

	public SingleAdapterServerMergeStrategy() {}

	public SingleAdapterServerMergeStrategy(
			final short internalAdapterId,
			final SampleModel sampleModel,
			final RasterTileMergeStrategy<T> mergeStrategy ) {
		this.internalAdapterId = internalAdapterId;
		this.sampleModel = sampleModel;
		this.mergeStrategy = mergeStrategy;
	}

	@SuppressFBWarnings(value = {
		"DLS_DEAD_LOCAL_STORE"
	}, justification = "Incorrect warning, sampleModelBinary used")
	@Override
	public byte[] toBinary() {

		final SerializableState serializableSampleModel = SerializerFactory.getState(sampleModel);
		byte[] sampleModelBinary = new byte[0];
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableSampleModel);
			oos.close();
			sampleModelBinary = baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize sample model",
					e);
		}

		final byte[] mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);

		int byteCount = sampleModelBinary.length + 4 + 2 + mergeStrategyBinary.length + 4;
		final ByteBuffer buf = ByteBuffer.allocate(byteCount);
		buf.putInt(sampleModelBinary.length);
		buf.put(sampleModelBinary);
		buf.putShort(internalAdapterId);
		buf.putInt(mergeStrategyBinary.length);
		buf.put(mergeStrategyBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);

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
				if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof SampleModel)) {
					sampleModel = (SampleModel) ((SerializableState) o).getObject();

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

		internalAdapterId = buf.getShort();

		final byte[] mergeStrategyBinary = new byte[buf.getInt()];
		if (mergeStrategyBinary.length > 0) {
			try {
				buf.get(mergeStrategyBinary);
				mergeStrategy = (RasterTileMergeStrategy) PersistenceUtils.fromBinary(mergeStrategyBinary);

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

	@Override
	public void merge(
			RasterTile thisTile,
			RasterTile nextTile,
			short internalAdapterId ) {
		if (mergeStrategy != null) {
			mergeStrategy.merge(
					thisTile,
					nextTile,
					sampleModel);
		}
	}

	public T getMetadata(
			final GridCoverage tileGridCoverage,
			final Map originalCoverageProperties,
			final RasterDataAdapter dataAdapter ) {
		if (mergeStrategy != null) {
			return mergeStrategy.getMetadata(
					tileGridCoverage,
					dataAdapter);
		}
		return null;
	}

}