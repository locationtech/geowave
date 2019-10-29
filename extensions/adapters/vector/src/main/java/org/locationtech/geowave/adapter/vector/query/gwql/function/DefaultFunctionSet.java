package org.locationtech.geowave.adapter.vector.query.gwql.function;

/**
 * The built-in set of functions used by the GeoWave query language.
 */
public class DefaultFunctionSet implements QLFunctionRegistrySpi {

  @Override
  public QLFunctionNameAndConstructor[] getSupportedPersistables() {
    return new QLFunctionNameAndConstructor[] {
        new QLFunctionNameAndConstructor("COUNT", CountFunction::new),
        new QLFunctionNameAndConstructor("BBOX", BboxFunction::new),
        new QLFunctionNameAndConstructor("MIN", MinFunction::new),
        new QLFunctionNameAndConstructor("MAX", MaxFunction::new),
        new QLFunctionNameAndConstructor("SUM", SumFunction::new)};
  }

}
