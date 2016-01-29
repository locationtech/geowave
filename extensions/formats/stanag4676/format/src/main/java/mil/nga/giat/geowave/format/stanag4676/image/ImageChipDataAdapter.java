package mil.nga.giat.geowave.format.stanag4676.image;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class ImageChipDataAdapter implements
		WritableDataAdapter<ImageChip>
{
	public final static ByteArrayId ADAPTER_ID = new ByteArrayId(
			"image");
	private final static ByteArrayId IMAGE_FIELD_ID = new ByteArrayId(
			"image");
	private final FieldVisibilityHandler<ImageChip, Object> imageChipVisibilityHandler;

	public ImageChipDataAdapter() {
		this(
				null);
	}

	public ImageChipDataAdapter(
			final FieldVisibilityHandler<ImageChip, Object> imageChipVisibilityHandler ) {
		this.imageChipVisibilityHandler = imageChipVisibilityHandler;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return ADAPTER_ID;
	}

	@Override
	public boolean isSupported(
			final ImageChip entry ) {
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			final ImageChip entry ) {
		return entry.getDataId();
	}

	@Override
	public ImageChip decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		return ImageChipUtils.fromDataIdAndValue(
				data.getDataId(),
				(byte[]) data.getAdapterExtendedData().getValue(
						IMAGE_FIELD_ID));
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final ImageChip entry,
			final CommonIndexModel indexModel ) {
		final Map<ByteArrayId, Object> fieldIdToValueMap = new HashMap<ByteArrayId, Object>();
		fieldIdToValueMap.put(
				IMAGE_FIELD_ID,
				entry.getImageBinary());
		return new AdapterPersistenceEncoding(
				getAdapterId(),
				entry.getDataId(),
				new PersistentDataset<CommonIndexValue>(),
				new PersistentDataset<Object>(
						fieldIdToValueMap));
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		if (IMAGE_FIELD_ID.equals(fieldId)) {
			return (FieldReader) FieldUtils.getDefaultReaderForClass(byte[].class);
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public FieldWriter<ImageChip, Object> getWriter(
			final ByteArrayId fieldId ) {
		if (IMAGE_FIELD_ID.equals(fieldId)) {
			if (imageChipVisibilityHandler != null) {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(
						byte[].class,
						imageChipVisibilityHandler);
			}
			else {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(byte[].class);
			}
		}
		return null;
	}

}
