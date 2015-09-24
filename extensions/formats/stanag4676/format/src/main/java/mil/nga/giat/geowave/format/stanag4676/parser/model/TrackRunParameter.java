package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class TrackRunParameter
{
	private Long id;

	private String name;
	private String value;

	public TrackRunParameter() {}

	public TrackRunParameter(
			String name,
			String value ) {
		this.name = name;
		this.value = value;
	}

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(
			String value ) {
		this.value = value;
	}
}
