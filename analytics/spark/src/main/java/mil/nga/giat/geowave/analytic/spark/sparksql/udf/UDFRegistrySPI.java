package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import java.util.Objects;
import java.util.function.Supplier;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomContains;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomCovers;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomCrosses;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomDisjoint;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomEquals;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomIntersects;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomOverlaps;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomTouches;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.WKTGeomWithin;

public class UDFRegistrySPI
{
	public static UDFNameAndConstructor[] getSupportedUDFs() {
		return new UDFNameAndConstructor[] {
			new UDFNameAndConstructor(
					new String[] {"GeomContains", "WKTGeomContains"},
					GeomContains::new,
					WKTGeomContains::new),
			new UDFNameAndConstructor(
					new String[] {"GeomCovers", "WKTGeomCovers"},
					GeomCovers::new,
					WKTGeomCovers::new),
			new UDFNameAndConstructor(
					new String[] {"GeomCrosses", "WKTGeomCrosses"},
					GeomCrosses::new,
					WKTGeomCrosses::new),
			new UDFNameAndConstructor(
					new String[] {"GeomDisjoint", "WKTGeomDisjoint"},
					GeomDisjoint::new,
					WKTGeomDisjoint::new),
			new UDFNameAndConstructor(
					new String[] {"GeomEquals", "WKTGeomEquals"},
					GeomEquals::new,
					WKTGeomEquals::new),
			new UDFNameAndConstructor(
					new String[] {"GeomIntersects", "WKTGeomIntersects"},
					GeomIntersects::new,
					WKTGeomIntersects::new),
			new UDFNameAndConstructor(
					new String[] {"GeomOverlaps", "WKTGeomOverlaps"},
					GeomOverlaps::new,
					WKTGeomOverlaps::new),
			new UDFNameAndConstructor(
					new String[] {"GeomTouches", "WKTGeomTouches"},
					GeomTouches::new,
					WKTGeomTouches::new),
			new UDFNameAndConstructor(
					new String[] {"GeomWithin", "WKTGeomWithin"},
					GeomWithin::new,
					WKTGeomWithin::new),
			new UDFNameAndConstructor(
					new String[] {"GeomWithinDistance", "GeomDistance", "WKTGeomDistance"},
					GeomWithinDistance::new,
					null)
		};
	}

	public static UDFNameAndConstructor findFunctionByName(
			String udfName ) {
		UDFNameAndConstructor[] udfFunctions = UDFRegistrySPI.getSupportedUDFs();
		for (int iUDF = 0; iUDF < udfFunctions.length; iUDF += 1) {
			UDFNameAndConstructor compare = udfFunctions[iUDF];
			if (compare.nameMatch(udfName)) {
				return compare;
			}
		}
		return null;
	}

	public static class UDFNameAndConstructor
	{
		private final String[] udfNames;
		private final Supplier<GeomFunction> predicateConstructor;
		private final Supplier<WKTGeomFunction> wktPredConstructor;

		public UDFNameAndConstructor(
				final String[] udfNames,
				final Supplier<GeomFunction> predicateConstructor,
				final Supplier<WKTGeomFunction> wktPredConstructor ) {
			this.udfNames = udfNames;
			this.predicateConstructor = predicateConstructor;
			this.wktPredConstructor = wktPredConstructor;
		}

		public String[] getUDFNames() {
			return udfNames;
		}

		public boolean nameMatch(
				String udfName ) {
			for (int iName = 0; iName < udfNames.length; iName += 1) {
				if (Objects.equals(
						udfNames[iName],
						udfName)) {
					return true;
				}
			}
			return false;
		}

		public Supplier<GeomFunction> getPredicateConstructor() {
			return predicateConstructor;
		}

		public Supplier<WKTGeomFunction> getWKTConstructor() {
			return wktPredConstructor;
		}
	}

}
