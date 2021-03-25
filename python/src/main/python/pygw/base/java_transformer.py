#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

class JavaTransformer:
    """
    Base class for transforming Java objects to an appropriate Python counterpart.  All extending classes should
    overwrite the `transform` method to perform the transformation.
    """

    def transform(self, j_object):
        """
        Transforms a Java object into a Python-friendly variant.

        Args:
            j_object (java object): The java object to transform.
        Raises:
            NotImplementedError: This is a base class and not intended to be used directly.
        Returns:
            A Python-friendly equivalent of the query result.
        """
        raise NotImplementedError


class NoOpTransformer(JavaTransformer):
    """
    Transformer that passes through the Java object.
    """

    def transform(self, j_object):
        """
        Pass through the given java object.

        Args:
            j_object (Java Object): An Java object.
        Returns:
            The Java object.
        """
        return j_object
