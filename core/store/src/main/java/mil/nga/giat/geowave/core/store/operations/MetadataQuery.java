package mil.nga.giat.geowave.core.store.operations;

public class MetadataQuery
{
	private final byte[] primaryId;
	private final byte[] secondaryId;
	private final String[] authorizations;

	public MetadataQuery(
			final byte[] primaryId,
			final byte[] secondaryId,
			final String... authorizations ) {
		this.primaryId = primaryId;
		this.secondaryId = secondaryId;
		this.authorizations = authorizations;
	}

	public byte[] getPrimaryId() {
		return primaryId;
	}

	public byte[] getSecondaryId() {
		return secondaryId;
	}

	public boolean hasPrimaryId() {
		return (primaryId != null) && (primaryId.length > 0);
	}

	public boolean hasSecondaryId() {
		return (secondaryId != null) && (secondaryId.length > 0);
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

}
