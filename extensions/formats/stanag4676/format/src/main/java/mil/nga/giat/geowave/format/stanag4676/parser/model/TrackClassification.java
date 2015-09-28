package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
public class TrackClassification
{
	private Long id;
	/**
	 * an estimate of the classification of an object being tracked
	 */
	public ObjectClassification classification;

	/**
	 * the source(s) used to determine/estimate the classification
	 */
	public ModalityType source;

	/**
	 * A measure of confidence that a correct object classification is made.
	 * <p>
	 * NOTE: This value is set only when the classification is not UNKNOWN
	 * <p>
	 * Provides a level of confidence or certainty. Allowed values are 0 to 100,
	 * indicating a percentage of certainty. No guidance is imposed on how this
	 * percentage is calculated, as it will vary depending on the class from
	 * which the enumeration is called. The value 0 indicates no confidence; a
	 * value of 100 indicates the highest possible confidence. This field is
	 * intended to be analogous to credibility (of information) criteria
	 * specified in AJP 2.1, whose values range from 1 to 6, but no assignment
	 * of qualitative confidence statements is imposed on specific ranges of
	 * percentages.
	 */
	public int valueConfidence;

	/**
	 * A measure of reliability of the source used to determine/estimate the
	 * classification
	 * <p>
	 * NOTE: This value is set only when the classification is not UNKNOWN
	 * <p>
	 * Provides a measure of confidence in the reliability of the source that
	 * generated the confidence value. Source may be a person, algorithm,
	 * exploitation/tracker system, or unit/organization. Allowed values are 0
	 * to 100. The value 0 indicates no reliability; a value of 100 indicates
	 * the highest possible reliability. This field is intended to be analogous
	 * to reliability (of source) criteria specified in AJP 2.1, whose values
	 * range from A to F, but no assignment of qualitative reliability
	 * statements is imposed on specific ranges of percentages.
	 */
	public int sourceReliability;

	/**
	 * the estimated number of objects or entities represented by the track.
	 * <p>
	 * maps to Link 16 term "strength" but reports actual number of estimated
	 * entities versus a range of entities.
	 */
	public int numObjects;

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public ObjectClassification getClassification() {
		return classification;
	}

	public void setClassification(
			ObjectClassification classification ) {
		this.classification = classification;
	}

	public ModalityType getSource() {
		return source;
	}

	public void setSource(
			ModalityType source ) {
		this.source = source;
	}

	public int getValueConfidence() {
		return valueConfidence;
	}

	public void setValueConfidence(
			int valueConfidence ) {
		this.valueConfidence = valueConfidence;
	}

	public int getSourceReliability() {
		return sourceReliability;
	}

	public void setSourceReliability(
			int sourceReliability ) {
		this.sourceReliability = sourceReliability;
	}

	public int getNumObjects() {
		return numObjects;
	}

	public void setNumObjects(
			int numObjects ) {
		this.numObjects = numObjects;
	}
}
