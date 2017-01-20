package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class Security
{
	private ClassificationLevel classification;
	private String policyName;
	private String controlSystem;
	private String dissemination;
	private String releasability;

	public ClassificationLevel getClassification() {
		return classification;
	}

	public void setClassification(
			ClassificationLevel classification ) {
		this.classification = classification;
	}

	public String getPolicyName() {
		return policyName;
	}

	public void setPolicyName(
			String policyName ) {
		this.policyName = policyName;
	}

	public String getControlSystem() {
		return controlSystem;
	}

	public void setControlSystem(
			String controlSystem ) {
		this.controlSystem = controlSystem;
	}

	public String getDissemination() {
		return dissemination;
	}

	public void setDissemination(
			String dissemination ) {
		this.dissemination = dissemination;
	}

	public String getReleasability() {
		return releasability;
	}

	public void setReleasability(
			String releasability ) {
		this.releasability = releasability;
	}

}
