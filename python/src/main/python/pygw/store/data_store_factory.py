#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg

from .data_store import DataStore
from .data_store_options import DataStoreOptions

class DataStoreFactory():
    """
    Factory class for creating a data store from a given set of options.
    """

    @classmethod
    def create_data_store(cls, options):
        """
        Creates a data store from a set of options for a specific backend type.

        Args:
            options (pygw.store.data_store_options.DataStoreOptions): The options for the data store.
        Returns:
            The `pygw.store.data_store.DataStore` referenced by the given options.
        """
        assert isinstance(options, DataStoreOptions)
        j_ds = geowave_pkg.core.store.api.DataStoreFactory.createDataStore(options._java_ref)
        return DataStore(j_ds)
