package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public abstract class BaseSecondaryIndexDataStore<MutationType> implements
		SecondaryIndexDataStore,
		Closeable
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

	protected static DistributableQueryFilter getFilter(
			final List<DistributableQueryFilter> constraints ) {
		final DistributableQueryFilter filter;
		if (constraints.isEmpty()) {
			filter = null;
		}
		else if (constraints.size() == 1) {
			filter = constraints.get(0);
		}
		else {
			filter = new DistributableFilterList(
					false,
					constraints);
		}
		return filter;
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
			try {
				writer.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close secondary index writer",
						e);
			}
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
