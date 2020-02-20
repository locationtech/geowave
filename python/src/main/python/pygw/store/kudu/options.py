#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg
from pygw.store import DataStoreOptions

class KuduOptions(DataStoreOptions):
    """
    Kudu data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.kudu.config.KuduRequiredOptions())

    def set_kudu_master(self, kudu_master):
        """
        Sets the URL for the Kudu master node.

        Args:
            kudu_master (str): The URL for the Kudu master node.
        """
        self._java_ref.setKuduMaster(kudu_master)

    def get_kudu_master(self):
        """
        Returns:
            The URL for the Kudu master node.
        """
        return self._java_ref.getKuduMaster()
