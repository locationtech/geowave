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

class AccumuloOptions(DataStoreOptions):
    """
    Accumulo data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.accumulo.config.AccumuloRequiredOptions())

    def set_zookeeper(self, zookeeper):
        """
        Sets the list of Zookeper servers that the Accumulo instance uses as a comma-separated
        string.

        Args:
            zookeeper (str): A comma-separated list of Zookeeper servers.
        """
        self._java_ref.setZookeeper(zookeeper)

    def get_zookeeper(self):
        """
        Returns:
            A comma-separated list of Zookeper servers.
        """
        return self._java_ref.getZookeeper()

    def set_instance(self, instance):
        """
        Sets the Accumulo instance ID to use for the data store.

        Args:
            instance (str): The Accumulo instance ID to use.
        """
        self._java_ref.setInstance(instance)

    def get_instance(self):
        """
        Returns:
            The Accumulo instance ID.
        """
        return self._java_ref.getInstance()

    def set_user(self, user):
        """
        Sets the Accumulo user ID.

        Args:
            user (str): The Accumulo user ID.
        """
        self._java_ref.setUser(user)

    def get_user(self):
        """
        Returns:
            The Accumulo user ID.
        """
        return self._java_ref.getUser()

    def set_password(self, password):
        """
        Sets the Accumulo password.

        Args:
            password (str): The Accumulo password.
        """
        self._java_ref.setPassword(password)

    def get_password(self):
        """
        Returns:
            The Accumulo password.
        """
        return self._java_ref.getPassword()

    def set_use_locality_groups(self, use_locality_groups):
        """
        Sets whether or not to use locality groups.

        Args:
            use_locality_groups (bool): Whether or not to use locality groups.
        """
        self._base_options.setUseLocalityGroups(use_locality_groups)

    def is_use_locality_groups(self):
        """
        Returns:
            True if locality groups are enabled, False otherwise.
        """
        return self._base_options.isUseLocalityGroups()
