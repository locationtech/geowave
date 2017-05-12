package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSecondaryIndexDataStore<MutationType> implements
		SecondaryIndexDataStore,
		Closeable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(BaseSecondaryIndexDataStore.class);
	protected final Map<String, Writer<MutationType>> writerCache = new HashMap<>();
	protected final static byte[] EMPTY_VALUE = new byte[0];

	public BaseSecondaryIndexDataStore() {}

	@Override
	public void storeJoinEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId,
			final ByteArrayId attributeVisibility ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndexId);
			if (writer != null) {
				writer.write(buildJoinMutation(
						indexedAttributeValue.getBytes(),
						adapterId.getBytes(),
						indexedAttributeFieldId.getBytes(),
						primaryIndexId.getBytes(),
						primaryIndexRowId.getBytes(),
						attributeVisibility.getBytes()));
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to build secondary index row mutation.",
					e);
		}
	}

	@Override
	public void storeEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId dataId,
			final ByteArrayId attributeVisibility,
			final List<FieldInfo<?>> attributes ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndexId);
			if (writer != null) {
				for (final FieldInfo<?> indexedAttribute : attributes) {
					writer.write(buildMutation(
							indexedAttributeValue.getBytes(),
							adapterId.getBytes(),
							indexedAttributeFieldId.getBytes(),
							dataId.getBytes(),
							indexedAttribute.getDataValue().getId().getBytes(),
							indexedAttribute.getWrittenValue(),
							indexedAttribute.getVisibility()));
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
	public void deleteJoinEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndexId);
			if (writer != null) {
				writer.write(buildJoinDeleteMutation(
						indexedAttributeValue.getBytes(),
						adapterId.getBytes(),
						indexedAttributeFieldId.getBytes(),
						primaryIndexId.getBytes(),
						primaryIndexRowId.getBytes()));
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed to delete from secondary index.",
					e);
		}
	}

	@Override
	public void deleteEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId dataId,
			final List<FieldInfo<?>> attributes ) {
		try {
			final Writer<MutationType> writer = getWriter(secondaryIndexId);
			if (writer != null) {
				for (FieldInfo<?> attribute : attributes) {
					writer.write(buildFullDeleteMutation(
							indexedAttributeValue.getBytes(),
							adapterId.getBytes(),
							indexedAttributeFieldId.getBytes(),
							dataId.getBytes(),
							attribute.getDataValue().getId().getBytes()));
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
		writerCache.clear();
	}

	@Override
	public void flush() {
		close();
	}

	protected abstract MutationType buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId,
			final byte[] attributeVisibility )
			throws IOException;

	protected abstract MutationType buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility )
			throws IOException;

	protected abstract MutationType buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws IOException;

	protected abstract MutationType buildFullDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId )
			throws IOException;

	protected abstract Writer<MutationType> getWriter(
			ByteArrayId secondaryIndexId );

}