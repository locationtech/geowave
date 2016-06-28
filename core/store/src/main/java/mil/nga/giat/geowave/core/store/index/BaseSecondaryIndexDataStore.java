package mil.nga.giat.geowave.core.store.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.Closable;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.Writer;

public abstract class BaseSecondaryIndexDataStore<MutationType> implements
		SecondaryIndexDataStore,
		Closable
{

	private final static Logger LOGGER = Logger.getLogger(BaseSecondaryIndexDataStore.class);
	protected static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	protected final Map<String, Writer<MutationType>> writerCache = new HashMap<>();

	public BaseSecondaryIndexDataStore() {}

	@Override
	public void store(
			final SecondaryIndex<?> secondaryIndex,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId,
			final List<FieldInfo<?>> indexedAttributes ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndex);
			if (writer != null) {
				for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
					@SuppressWarnings("unchecked")
					final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex
							.getIndexStrategy()
							.getInsertionIds(
									Arrays.asList(indexedAttribute));
					for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
						writer.write(buildMutation(
								insertionId.getBytes(),
								secondaryIndex.getId().getBytes(),
								indexedAttribute.getDataValue().getId().getBytes(),
								indexedAttribute.getWrittenValue(),
								indexedAttribute.getVisibility(),
								primaryIndexId.getBytes(),
								primaryIndexRowId.getBytes()));
					}
				}
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to build secondary index row mutation.",
					e);
		}
	}

	@Override
	public void delete(
			final SecondaryIndex<?> secondaryIndex,
			final List<FieldInfo<?>> indexedAttributes ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndex);
			if (writer != null) {
				for (final FieldInfo<?> indexedAttribute : indexedAttributes) {
					@SuppressWarnings("unchecked")
					final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex
							.getIndexStrategy()
							.getInsertionIds(
									Arrays.asList(indexedAttribute));
					for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
						writer.write(buildDeleteMutation(
								insertionId.getBytes(),
								secondaryIndex.getId().getBytes(),
								indexedAttribute.getDataValue().getId().getBytes()));
					}
				}
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed to delete from secondary index.",
					e);
		}
	}

	@Override
	public void removeAll() {
		close();
		writerCache.clear();
	}

	@Override
	public void close() {
		for (final Writer<MutationType> writer : writerCache.values()) {
			writer.close();
		}
	}

	@Override
	public void flush() {
		close();
	}

	protected abstract MutationType buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName,
			final byte[] attributeValue,
			final byte[] visibility,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws Exception;

	protected abstract MutationType buildDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] secondaryIndexId,
			final byte[] attributeName )
			throws Exception;

	protected abstract Writer<MutationType> getWriter(
			SecondaryIndex<?> secondaryIndex );

}
