package mil.nga.giat.geowave.types.stanag4676.service;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/")
public class Stanag4676ImageryChipApplication extends
		Application
{

	@Override
	public Set<Class<?>> getClasses() {
		final Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(mil.nga.giat.geowave.types.stanag4676.service.rest.Stanag4676ImageryChipService.class);
		return classes;
	}

}
