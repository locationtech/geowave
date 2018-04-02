package mil.nga.giat.geowave.core.store.entities;

public class GeoWaveMetadata
{
	private final byte[] primaryId;
	private final byte[] secondaryId;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveMetadata(
			final byte[] primaryId,
			final byte[] secondaryId,
			final byte[] visibility,
			final byte[] value ) {
		this.primaryId = primaryId;
		this.secondaryId = secondaryId;
		this.visibility = visibility;
		this.value = value;
	}

	public byte[] getPrimaryId() {
		return primaryId;
	}

	public byte[] getSecondaryId() {
		return secondaryId;
	}

	public byte[] getVisibility() {
		return visibility;
	}

	public byte[] getValue() {
		return value;
	}
}
