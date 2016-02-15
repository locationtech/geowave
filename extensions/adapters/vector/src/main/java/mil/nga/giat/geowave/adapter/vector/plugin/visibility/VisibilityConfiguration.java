package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import java.io.ObjectStreamException;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * 
 * Describes which attribute in a feature contains the visibility constraints,
 * interpreted by a {@link ColumnVisibilityManagementSpi}
 * 
 */
public class VisibilityConfiguration implements
		SimpleFeatureUserDataConfiguration
{

	private static final long serialVersionUID = -664252700036603897L;
	private String attributeName = "GEOWAVE_VISIBILITY";
	private String managerClassName = JsonDefinitionColumnVisibilityManagement.class.toString();
	private transient VisibilityManagement<SimpleFeature> manager = new JsonDefinitionColumnVisibilityManagement<SimpleFeature>();

	public VisibilityConfiguration() {

	}

	public VisibilityConfiguration(
			final SimpleFeatureType type ) {
		this.configureFromType(type);
	}

	public void updateWithDefaultIfNeeded(
			final SimpleFeatureType type,
			final VisibilityManagement<SimpleFeature> manager ) {
		if (!configureManager(type) && manager != null) {
			this.manager = manager;
			this.managerClassName = manager.getClass().getName();
			this.updateType(type);
		}
	}

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(
			final String attributeName ) {
		if (attributeName == null) return;
		this.attributeName = attributeName;
	}

	public String getManagerClassName() {
		return managerClassName;
	}

	public void setManagerClassName(
			String managerClassName ) {
		this.managerClassName = managerClassName;
	}

	@JsonIgnore
	public VisibilityManagement<SimpleFeature> getManager() {
		return manager;
	}

	@Override
	public void updateType(
			final SimpleFeatureType persistType ) {
		for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
			attrDesc.getUserData().remove(
					"visibility");
		}
		final AttributeDescriptor attrDesc = persistType.getDescriptor(attributeName);
		if (attrDesc != null) {
			attrDesc.getUserData().put(
					"visibility",
					Boolean.TRUE);
		}
		persistType.getUserData().put(
				"visibilityManagerClass",
				this.managerClassName);
	}

	@Override
	public void configureFromType(
			final SimpleFeatureType persistType ) {
		for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
			if (attrDesc.getUserData().containsKey(
					"visibility") && Boolean.TRUE.equals(attrDesc.getUserData().get(
					"visibility"))) {
				attributeName = attrDesc.getLocalName();
			}
		}
		configureManager(persistType);
	}

	@SuppressWarnings("unchecked")
	private boolean configureManager(
			final SimpleFeatureType persistType ) {

		final Object vm = persistType.getUserData().get(
				"visibilityManagerClass");
		if (vm != null) {
			if (managerClassName == null || !vm.toString().equals(
					this.managerClassName)) {
				try {
					managerClassName = vm.toString();

					manager = (VisibilityManagement<SimpleFeature>) Class.forName(
							vm.toString()).newInstance();
				}
				catch (Exception ex) {
					VisibilityManagementHelper.LOGGER.warn("Cannot load visibility management class " + vm.toString());
					return false;
				}
			}
			return true;
		}
		return false;

	}

	@SuppressWarnings("unchecked")
	public Object readResolve()
			throws ObjectStreamException {
		if (managerClassName != null && !manager.getClass().getName().equals(
				managerClassName)) {
			try {
				manager = (VisibilityManagement<SimpleFeature>) Class.forName(
						managerClassName).newInstance();
			}
			catch (Exception ex) {
				VisibilityManagementHelper.LOGGER.warn("Cannot load visibility management class " + managerClassName);
			}
		}
		return this;
	}
}
