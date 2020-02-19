#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from datetime import datetime

from shapely.geometry.base import BaseGeometry

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import GeometryType
from pygw.config import java_gateway
from pygw.config import java_pkg
from pygw.config import reflection_util

def _j_match_action(match_action):
    return java_pkg.org.opengis.filter.MultiValuedFilter.MatchAction.valueOf(match_action.upper())

# These functions are needed in order to invoke java methods that are named with
# reserved python keywords such as and, or, and not
def _invoke_filter_list_method_by_name(j_filter_factory, name, filters):
    filter_factory_class = j_filter_factory.getClass()
    list_class = reflection_util.classForName("java.util.List")
    class_array = java_gateway.new_array(java_pkg.java.lang.Class, 1)
    class_array[0] = list_class
    method = filter_factory_class.getMethod(name, class_array)
    filter_list = java_pkg.java.util.ArrayList()
    for filter in filters:
        filter_list.append(filter)
    objects_array = java_gateway.new_array(java_pkg.java.lang.Object, 1)
    objects_array[0] = filter_list
    return method.invoke(j_filter_factory, objects_array)

def _invoke_filter_method_by_name(j_filter_factory, name, filter):
    filter_factory_class = j_filter_factory.getClass()
    filter_class = reflection_util.classForName("org.opengis.filter.Filter")
    class_array = java_gateway.new_array(java_pkg.java.lang.Class, 1)
    class_array[0] = filter_class
    method = filter_factory_class.getMethod(name, class_array)
    objects_array = java_gateway.new_array(java_pkg.java.lang.Object, 1)
    objects_array[0] = filter
    return method.invoke(j_filter_factory, objects_array)

class FilterFactory(GeoWaveObject):
    """
    Filter factory for constructing filters to be used in vector queries. Methods
    of this factory generally return either a Filter or Expression which can be used
    in additional method calls.
    """

    def __init__(self):
        j_filter_factory = java_pkg.org.geotools.filter.FilterFactoryImpl()
        super().__init__(j_filter_factory)

    def id(self, fids):
        """
        Constructs a filter that matches a set of feature IDs.

        Args:
            fids (list of str): The list of feature IDs to match.
        Returns:
            A Filter with the given feature IDs.
        """
        j_fids = java_gateway.new_array(java_pkg.org.opengis.filter.identity.FeatureId, len(fids))
        for idx, fid in enumerate(fids):
            if isinstance(fid, str):
                j_fids[idx] = self.feature_id(fid)
            else:
                j_fids[idx] = fid
        return self._java_ref.id(j_fids)

    def feature_id(self, id):
        """
        Constructs a filter that matches a specific feature ID.

        Args:
            id (str): The feature ID.
        Returns:
            A Filter with the given feature ID.
        """
        return self._java_ref.featureId(id)

    def gml_object_id(self, id):
        """
        Constructs a filter that matches a specific gml object ID.

        Args:
            id (str): The gml object ID.
        Returns:
            A Filter with the given gml object ID.
        """
        return self._java_ref.gmlObjectId(id)

    def property(self, name):
        """
        Constructs an expression that references the given property name.

        Args:
            name (str): The property name.
        Returns:
            An Expression with the given property name.
        """
        return self._java_ref.property(name)

    def literal(self, value):
        """
        Constructs an expression with the given literal value.

        Args:
            value (any): The literal value to use.
        Returns:
            An Expression with the given literal value.
        """
        if isinstance(value, datetime):
            # Convert the date to a string
            value = value.strftime("%Y-%m-%dT%H:%M:%S")
        if isinstance(value, str):
            # Prevent Py4J from assuming the string matches up with the char variant method
            filter_factory_class = self._java_ref.getClass()
            object_class = reflection_util.classForName("java.lang.Object")
            class_array = java_gateway.new_array(java_pkg.java.lang.Class, 1)
            class_array[0] = object_class
            method = filter_factory_class.getMethod("literal", class_array)
            objects_array = java_gateway.new_array(java_pkg.java.lang.Object, 1)
            objects_array[0] = value
            return method.invoke(self._java_ref, objects_array)
        if isinstance(value, BaseGeometry):
            return self._java_ref.literal(GeometryType().to_java(value))
        return self._java_ref.literal(value)

    def add(self, expr1, expr2):
        """
        Constructs an expression which adds two other expressions.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
        Returns:
            An Expression which represents [expr1 + expr2].
        """
        return self._java_ref.add(expr1, expr2)

    def subtract(self, expr1, expr2):
        """
        Constructs an expression which subtracts one expression from another.

        Args:
            expr1 (Expression): The expression to subtract from.
            expr2 (Expression): The expression to subtract.
        Returns:
            An Expression which represents [expr1 - expr2].
        """
        return self._java_ref.subtract(expr1, expr2)

    def multiply(self, expr1, expr2):
        """
        Constructs an expression which multiplies two other expressions.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
        Returns:
            An Expression which represents [expr1 * expr2].
        """
        return self._java_ref.multiply(expr1, expr2)

    def divide(self, expr1, expr2):
        """
        Constructs an expression which divides one expression by another.

        Args:
            expr1 (Expression): The expression to divide.
            expr2 (Expression): The expression to divide by.
        Returns:
            An Expression which represents [expr1 / expr2].
        """
        return self._java_ref.divide(expr1, expr2)

    def function(self, name, expressions):
        """
        Constructs an expression by passing a set of expressions to an expression function.

        Args:
            name (str): The name of the function.
            expressions (list of Expression): The expressions to use in the function.
        Returns:
            An Expression which represents the result of the function.
        """
        j_expressions = java_gateway.new_array(java_pkg.org.opengis.filter.expression.Expression, len(expressions))
        for idx, expression in enumerate(expressions):
            j_expressions[idx] = expression
        return self._java_ref.function(name, j_expressions)

    def and_(self, filters):
        """
        Constructs a filter which passes when all given filters pass.

        Args:
            filters (list of Filter): The filters to check.
        Returns:
            A Filter that passes when all given Filters pass.
        """
        return _invoke_filter_list_method_by_name(self._java_ref, "and", filters)

    def or_(self, filters):
        """
        Constructs a filter which passes when any of the given filters pass.

        Args:
            filters (list of Filter): The filters to check.
        Returns:
            A Filter that passes when one of the given Filters pass.
        """
        return _invoke_filter_list_method_by_name(self._java_ref, "or", filters)

    def not_(self, filter):
        """
        Constructs a filter that passes when the given filter does NOT pass.

        Args:
            filter (Filter): The filter to check.
        Returns:
            A Filter that passes when the given filter does NOT pass.
        """
        return _invoke_filter_method_by_name(self._java_ref, "not", filter)

    def between(self, expr, lower, upper, match_action=None):
        """
        Constructs a filter that passes when the given expression falls between a
        lower and upper expression.

        Args:
            expr (Expression): The expression to check.
            lower (Expression): The lower bound.
            upper (Expression): The upper bound.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the given expression falls between a
            lower and upper expression.
        """
        if match_action is None:
            return self._java_ref.between(expr, lower, upper)
        else:
            return self._java_ref.between(expr, lower, upper, _j_match_action(match_action))

    def equals(self, expr1, expr2):
        """
        Constructs a filter that passes when the given expressions are equal.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
        Returns:
            A Filter that passes when the given expressions are equal.
        """
        return self._java_ref.equals(expr1, expr2)

    def equal(self, expr1, expr2, match_case, match_action=None):
        """
        Constructs a filter that passes when the given expressions are equal.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the given expressions are equal.
        """
        if match_action is None:
            return self._java_ref.equal(expr1, expr2, match_case)
        else:
            return self._java_ref.equal(expr1, expr2, match_case, _j_match_action(match_action))

    def not_equals(self, expr1, expr2):
        """
        Constructs a filter that passes when the given expressions are NOT equal.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
        Returns:
            A Filter that passes when the given expressions are NOT equal.
        """
        return self._java_ref.notEqual(expr1, expr2)

    def not_equal(self, expr1, expr2, match_case, match_action=None):
        """
        Constructs a filter that passes when the given expressions are NOT equal.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the given expressions are NOT equal.
        """
        if match_action is None:
            return self._java_ref.notEqual(expr1, expr2, match_case)
        else:
            return self._java_ref.notEqual(expr1, expr2, match_case, _j_match_action(match_action))

    def greater(self, expr1, expr2, match_case=None, match_action=None):
        """
        Constructs a filter that passes when the first expression is greater than
        the second.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings. Default is None.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the first expression is greater than the
            second.
        """
        if match_case is None:
            return self._java_ref.greater(expr1, expr2)
        elif match_action is None:
            return self._java_ref.greater(expr1, expr2, match_case)
        else:
            return self._java_ref.greater(expr1, expr2, match_case, _j_match_action(match_action))

    def greater_or_equal(self, expr1, expr2, match_case=None, match_action=None):
        """
        Constructs a filter that passes when the first expression is greater than
        or equal to the second.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings. Default is None.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the first expression is greater than or equal
            to the second.
        """
        if match_case is None:
            return self._java_ref.greaterOrEqual(expr1, expr2)
        elif match_action is None:
            return self._java_ref.greaterOrEqual(expr1, expr2, match_case)
        else:
            return self._java_ref.greaterOrEqual(expr1, expr2, match_case, _j_match_action(match_action))

    def less(self, expr1, expr2, match_case=None, match_action=None):
        """
        Constructs a filter that passes when the first expression is less than
        the second.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings. Default is None.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the first expression is less than the
            second.
        """
        if match_case is None:
            return self._java_ref.less(expr1, expr2)
        elif match_action is None:
            return self._java_ref.less(expr1, expr2, match_case)
        else:
            return self._java_ref.less(expr1, expr2, match_case, _j_match_action(match_action))

    def less_or_equal(self, expr1, expr2, match_case=None, match_action=None):
        """
        Constructs a filter that passes when the first expression is les than
        or equal to the second.

        Args:
            expr1 (Expression): The first expression.
            expr2 (Expression): The second expression.
            match_case (bool): Whether or not to match case with strings. Default is None.
            match_action (str): The match action to use. Default is 'ANY'.
        Returns:
            A Filter that passes when the first expression is less than or equal
            to the second.
        """
        if match_case is None:
            return self._java_ref.lessOrEqual(expr1, expr2)
        elif match_action is None:
            return self._java_ref.lessOrEqual(expr1, expr2, match_case)
        else:
            return self._java_ref.lessOrEqual(expr1, expr2, match_case, _j_match_action(match_action))

    def like(self, expr, pattern, wildcard=None, single_char=None, escape=None, match_case=None, match_action=None):
        """
        Constructs a filter with character string comparison operator with pattern
        matching and specified wildcards.

        Args:
            expr (Expression): The expression to use.
            pattern (str): The pattern to match.
            wildcard (str): The string to use to match any characters.  Default is None.
            single_char (str): The string to use to match a single character.  Default is None.
            escape (str): The string to use to escape a wildcard.  Default is None.
            match_case (bool): Whether or not to match case with strings. Default is None.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when the first expression is greater than the
            second.
        """
        if wildcard is None:
            return self._java_ref.like(expr, pattern)
        elif match_case is None:
            return self._java_ref.like(expr, pattern, wildcard, single_char, escape)
        elif match_action is None:
            return self._java_ref.like(expr, pattern, wildcard, single_char, escape, match_case)
        else:
            return self._java_ref.like(expr, pattern, wildcard, single_char, escape, match_case, _j_match_action(match_action))

    def is_null(self, expr):
        """
        Constructs a filter that passes when the given expression is null.

        Args:
            expr (Expression): The expression to check.
        Returns:
            A Filter that passes when the given epxression is null.
        """
        return self._java_ref.isNull(expr)

    def bbox(self, geometry_expr, minx, miny, maxx, maxy, srs, match_action=None):
        """
        Constructs a filter that passes when the given geometry expression is within
        the given bounding box.

        Args:
            geometry_expr (Expression): An expression which represents a geometry.
            minx (float): The minimum X value of the bounding box.
            miny (float): The minimum Y value of the bounding box.
            maxx (float): The maximum X value of the bounding box.
            maxy (float): The maximum Y value of the bounding box.
            srs (str): The spatial reference system of the geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when the given geometry is within the bounding box.
        """
        if match_action is None:
            return self._java_ref.bbox(geometry_expr, minx*1.0, miny*1.0, maxx*1.0, maxy*1.0, srs)
        else:
            return self._java_ref.bbox(geometry_expr, minx*1.0, miny*1.0, maxx*1.0, maxy*1.0, srs, _j_match_action(match_action))

    def bbox_expr(self, geometry_expr, bbox_expr):
        """
        Constructs a filter that passes when the given geometry expression is within the
        given bounding box expression.

        Args:
            geometry_expr (Expression): An expression which represents a geometry.
            bbox_expr (Expression): An expression which represents a bounding box.
        Returns:
            A Filter that passes when the given geometry is within the bounding box.
        """
        return self._java_ref.bbox(geometry_expr, bbox_expr)

    def beyond(self, geometry_expr1, geometry_expr2, distance, units, match_action=None):
        """
        Constructs a filter that passes when a given geometry is beyond a certain distance from
        a second given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            distance (float): The distance to use.
            units (str): The distance unit.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when the geometry is beyond the distance from the second
            geometry.
        """
        if match_action is None:
            return self._java_ref.beyond(geometry_expr1, geometry_expr2, distance*1.0, units)
        else:
            return self._java_ref.beyond(geometry_expr1, geometry_expr2, distance*1.0, units, _j_match_action(match_action))

    def contains(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when the first geometry expression contains the
        second geometry expression.

        Args:
            geometry_expr1 (Expression): An expression which represents the geometry to check against.
            geometry_expr2 (Expression): An expression which represents the geometry to check.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when the second geometry is contained by the first.
        """
        if match_action is None:
            return self._java_ref.contains(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.contains(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def crosses(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry crosses another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry crosses another given geometry.
        """
        if match_action is None:
            return self._java_ref.crosses(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.crosses(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def disjoint(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry is disjoint to another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry is disjoint to another given geometry.
        """
        if match_action is None:
            return self._java_ref.disjoint(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.disjoint(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def intersects(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry intersects another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry intersects another given geometry.
        """
        if match_action is None:
            return self._java_ref.intersects(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.intersects(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def overlaps(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry overlaps another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry overlaps another given geometry.
        """
        if match_action is None:
            return self._java_ref.overlaps(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.overlaps(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def touches(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry touches another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry touches another given geometry.
        """
        if match_action is None:
            return self._java_ref.touches(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.touches(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def within(self, geometry_expr1, geometry_expr2, match_action=None):
        """
        Constructs a filter that passes when a given geometry is within another given geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry is within another given geometry.
        """
        if match_action is None:
            return self._java_ref.within(geometry_expr1, geometry_expr2)
        else:
            return self._java_ref.within(geometry_expr1, geometry_expr2, _j_match_action(match_action))

    def dwithin(self, geometry_expr1, geometry_expr2, distance, units, match_action=None):
        """
        Constructs a filter that passes when a given geometry is within the specified distance
        of the second geometry.

        Args:
            geometry_expr1 (Expression): An expression which represents a geometry.
            geometry_expr2 (Expression): An expression which represents a geometry.
            distance (float): The distance to use.
            units (str): The unit of distance.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given geometry is within the specified distance of the
            second geometry.
        """
        if match_action is None:
            return self._java_ref.dwithin(geometry_expr1, geometry_expr2, distance*1.0, units)
        else:
            return self._java_ref.dwithin(geometry_expr1, geometry_expr2, distance*1.0, units, _j_match_action(match_action))

    def after(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression occurs after
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression occurs after the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.after(expr1, expr2)
        else:
            return self._java_ref.after(expr1, expr2, _j_match_action(match_action))

    def any_interacts(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression interacts with
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression interacts with the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.anyInteracts(expr1, expr2)
        else:
            return self._java_ref.anyInteracts(expr1, expr2, _j_match_action(match_action))

    def before(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression occurs before
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression occurs before the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.before(expr1, expr2)
        else:
            return self._java_ref.before(expr1, expr2, _j_match_action(match_action))

    def begins(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression begins
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression begins the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.begins(expr1, expr2)
        else:
            return self._java_ref.begins(expr1, expr2, _j_match_action(match_action))

    def begun_by(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression is begun by
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression is begun by the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.begunBy(expr1, expr2)
        else:
            return self._java_ref.begunBy(expr1, expr2, _j_match_action(match_action))

    def during(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression occurs during
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression occurs during the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.during(expr1, expr2)
        else:
            return self._java_ref.during(expr1, expr2, _j_match_action(match_action))

    def ended_by(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression is ended by
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression is ended by the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.endedBy(expr1, expr2)
        else:
            return self._java_ref.endedBy(expr1, expr2, _j_match_action(match_action))

    def ends(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression ends
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression ends the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.ends(expr1, expr2)
        else:
            return self._java_ref.ends(expr1, expr2, _j_match_action(match_action))

    def meets(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression meets
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression meets the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.meets(expr1, expr2)
        else:
            return self._java_ref.meets(expr1, expr2, _j_match_action(match_action))

    def met_by(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression is met by
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression is met by the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.metBy(expr1, expr2)
        else:
            return self._java_ref.metBy(expr1, expr2, _j_match_action(match_action))

    def overlapped_by(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression is overlapped by
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression is overlapped by the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.overlappedBy(expr1, expr2)
        else:
            return self._java_ref.overlappedBy(expr1, expr2, _j_match_action(match_action))

    def tcontains(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression contains
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression contains the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.tcontains(expr1, expr2)
        else:
            return self._java_ref.tcontains(expr1, expr2, _j_match_action(match_action))

    def tequals(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression equals
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression equals the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.tequals(expr1, expr2)
        else:
            return self._java_ref.tequals(expr1, expr2, _j_match_action(match_action))

    def toverlaps(self, expr1, expr2, match_action=None):
        """
        Constructs a filter that passes when a given temporal expression overlaps
        a second temporal expression.

        Args:
            expr1 (Expression): The first temporal expression.
            expr2 (Expression): The second temporal expression.
            match_action (str): The match action to use.  Default is 'ANY'.
        Returns:
            A Filter that passes when a given temporal expression overlaps the
            second temporal expression.
        """
        if match_action is None:
            return self._java_ref.toverlaps(expr1, expr2)
        else:
            return self._java_ref.toverlaps(expr1, expr2, _j_match_action(match_action))
