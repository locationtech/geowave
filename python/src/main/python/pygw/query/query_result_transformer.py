#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

class QueryResultTransformer():
    """
    Base class for transforming GeoWave query results from Java objects to
    their Python counterparts.  All extending classes should overwrite the `transform`
    method to perform the transformation.
    """

    def transform(self, j_result):
        """
        Transforms a Java query result into a Python-friendly variant.

        Args:
            j_result (java object): The java object to transform.
        Raises:
            NotImplementedError: This is a base class and not intended to be used directly.
        Returns:
            A Python-friendly equivalent of the query result.
        """
        raise NotImplementedError
