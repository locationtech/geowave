package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.UUID;

public class TrackDotSource
{
	private Long id;
	private UUID gmtiUuid = null;
	private Double distance;

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public UUID getGmtiUuid() {
		return gmtiUuid;
	}

	public void setGmtiUuid(
			UUID gmtiUuid ) {
		this.gmtiUuid = gmtiUuid;
	}

	public Double getDistance() {
		return distance;
	}

	public void setDistance(
			Double distance ) {
		this.distance = distance;
	}
}
