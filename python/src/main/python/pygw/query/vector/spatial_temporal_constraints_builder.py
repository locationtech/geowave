#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import GeometryType
from pygw.base.type_conversions import DateType
from pygw.config import geowave_pkg

from ..query_constraints import QueryConstraints

class SpatialTemporalConstraintsBuilder(GeoWaveObject):
    """
    A class that wraps the functionality of the GeoWave SpatialTemporalConstraintsBuilder.
    This class should not be constructed directly and instead should be created using
    the vector query constraints factory.
    """

    def no_spatial_constraints(self):
        """
        Use no spatial constraints in the query.  This is the default.

        Returns:
            This spatial temporal constraints builder.
        """
        self._java_ref.noSpatialConstraints()
        return self

    def spatial_constraints(self, geometry):
        """
        Use the given geometry as a spatial constraint.

        Args:
            geometry (shapely.geometry.base.BaseGeometry): The geometry to use in
                the constraint.
        Returns:
            This spatial temporal constraints builder.
        """
        self._java_ref.spatialConstraints(GeometryType().to_java(geometry))
        return self

    def spatial_constraints_crs(self, crs_code):
        """
        Speciify the coordinate reference system to use for the constraint.

        Args:
            crs_code (str): The CRS code to use.
        Returns:
            This spatial temporal constraints builder.
        """
        self._java_ref.spatialConstraintsCrs(crs_code)
        return self

    def spatial_constraints_compare_operation(self, spatial_compare_op):
        """
        Specify the spatial compare operation to use in conjuction with the provided
        spatial constraint.  Default is `intersects`. Possible values are `contains`,
        `overlaps`, `intersects`, `touches`, `within`, `disjoint`, `crosses`, `equals`.

        Args:
            spatial_compare_op (str): The spatial compare operation to use.
        Returns:
            This spatial temporal constraints builder.
        """
        j_compare_op = geowave_pkg.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation.valueOf(spatial_compare_op.upper())
        self._java_ref.spatialConstraintsCompareOperation(j_compare_op)
        return self

    def no_temporal_constraints(self):
        """
        Use no temporal constraints with the query.  This is the default.

        Returns:
            This spatial temporal constraints builder.
        """
        self.java_ref.noTemporalConstraints()
        return self

    def add_time_range(self, start_time, end_time):
        """
        Add a time range constraint to the query.

        Args:
            start_time (datetime): The start time of the constraint (inclusive).
            end_time (datetime): The end time of the constraint (exclusive).
        Returns:
            This spatial temporal constraints builder.
        """
        dt = DateType()
        self._java_ref.addTimeRange(dt.to_java(start_time), dt.to_java(end_time))
        return self

    def build(self):
        """
        Builds the configured spatial temporal constraint.

        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the configured spatial/temporal constraints.
        """
        return QueryConstraints(self._java_ref.build())
