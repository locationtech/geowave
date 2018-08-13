package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class InternalDataAdapterWrapper<T> implements
		InternalDataAdapter<T>
{
	private WritableDataAdapter<T> adapter;
	private short internalAdapterId;

	public InternalDataAdapterWrapper(
			WritableDataAdapter<T> adapter,
			short internalAdapterId ) {
		this.adapter = adapter;
		this.internalAdapterId = internalAdapterId;
	}

	public FieldWriter<T, Object> getWriter(
			ByteArrayId fieldId ) {
		return adapter.getWriter(fieldId);
	}

	public short getInternalAdapterId() {
		return internalAdapterId;
	}

	public byte[] toBinary() {
		return adapter.toBinary();
	}

	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
		return adapter.getReader(fieldId);
	}

	public void fromBinary(
			byte[] bytes ) {
		adapter.fromBinary(bytes);
	}

	public ByteArrayId getAdapterId() {
		return adapter.getAdapterId();
	}

	public boolean isSupported(
			T entry ) {
		return adapter.isSupported(entry);
	}

	public ByteArrayId getDataId(
			T entry ) {
		return adapter.getDataId(entry);
	}

	public T decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index ) {
		return adapter.decode(
				data,
				index);
	}

	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel ) {
		AdapterPersistenceEncoding retVal = adapter.encode(
				entry,
				indexModel);
		retVal.setInternalAdapterId(internalAdapterId);
		return retVal;
	}

	public int getPositionOfOrderedField(
			CommonIndexModel model,
			ByteArrayId fieldId ) {
		return adapter.getPositionOfOrderedField(
				model,
				fieldId);
	}

	public ByteArrayId getFieldIdForPosition(
			CommonIndexModel model,
			int position ) {
		return adapter.getFieldIdForPosition(
				model,
				position);
	}

	public void init(
			PrimaryIndex... indices ) {
		adapter.init(indices);
	}

	@Override
	public WritableDataAdapter<?> getAdapter() {
		return adapter;
	}
}
