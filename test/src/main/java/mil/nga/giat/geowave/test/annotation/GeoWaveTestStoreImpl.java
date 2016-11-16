package mil.nga.giat.geowave.test.annotation;

import java.lang.annotation.Annotation;

public class GeoWaveTestStoreImpl implements
		GeoWaveTestStore
{
	private String namespace;
	private GeoWaveStoreType[] value;
	private String[] options;
	private Class<? extends Annotation> annotationType;

	public GeoWaveTestStoreImpl(
			final GeoWaveTestStore parent ) {
		namespace = parent.namespace();
		value = parent.value();
		options = parent.options();
		annotationType = parent.annotationType();
	}

	public GeoWaveTestStoreImpl(
			final String namespace,
			final GeoWaveStoreType[] value,
			final String[] options,
			final Class<? extends Annotation> annotationType ) {
		this.namespace = namespace;
		this.value = value;
		this.options = options;
		this.annotationType = annotationType;
	}

	public void setNamespace(
			final String namespace ) {
		this.namespace = namespace;
	}

	public void setValue(
			final GeoWaveStoreType[] value ) {
		this.value = value;
	}

	public void setOptions(
			final String[] options ) {
		this.options = options;
	}

	public void setAnnotationType(
			final Class<? extends Annotation> annotationType ) {
		this.annotationType = annotationType;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return annotationType;
	}

	@Override
	public GeoWaveStoreType[] value() {
		return value;
	}

	@Override
	public String namespace() {
		return namespace;
	}

	@Override
	public String[] options() {
		return options;
	}

}
