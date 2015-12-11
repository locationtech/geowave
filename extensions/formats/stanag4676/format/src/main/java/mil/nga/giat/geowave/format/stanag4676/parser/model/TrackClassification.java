package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
public class TrackClassification extends
		TrackItem
{

	/**
	 * an estimate of the classification of an object being tracked
	 */
	public ObjectClassification classification;

	/**
	 * credibility of classification
	 */
	public ClassificationCredibility credibility;

	/**
	 * the estimated number of objects or entities represented by the track.
	 * <p>
	 * maps to Link 16 term "strength" but reports actual number of estimated
	 * entities versus a range of entities.
	 */
	public int numObjects;

	public ObjectClassification getClassification() {
		return classification;
	}

	public void setClassification(
			ObjectClassification classification ) {
		this.classification = classification;
	}

	public ClassificationCredibility getCredibility() {
		return credibility;
	}

	public void setCredibility(
			ClassificationCredibility credibility ) {
		this.credibility = credibility;
	}

	public int getNumObjects() {
		return numObjects;
	}

	public void setNumObjects(
			int numObjects ) {
		this.numObjects = numObjects;
	}
}