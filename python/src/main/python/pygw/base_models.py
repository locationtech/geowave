class PyGwJavaWrapper:
    """[INTERNAL] Base Class for all PyGw Objects that wrap py4j objects"""

    """ sets gateway and java ref for wrapper
    Args:
        gateway [py4j]
        java_ref [Java ref] 
    """
    def __init__(self, gateway, java_ref):
        self._gateway = gateway
        self._java_ref = java_ref

    """ JavaRef of PyGW object
    Returns:
        String: Formatted string with PyGW class and JavaRef
    """
    def __repr__(self):
        return "PyGW {} Object with JavaRef@{}".format(self.__class__, self._java_ref)

    """ Equality of two PyGw Java wrappers
    Args:
        other [PyGwJavaWrapper]: A PyGwJavaWrapper instance
    Returns:
        bool: equality of this Java Wrapper and the other Java Wrapper
    """
    def __eq__(self, other):
        if not isinstance(other, PyGwJavaWrapper):
            return False
        return self._java_ref == other._java_ref
    
    def is_instance_of(self, java_class):
        return isinstance(self._gateway, self._java_ref, java_class)

class DataStore(PyGwJavaWrapper):
    """Wrapper to expose all of DataStore API"""

    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)
        self.interfacer = gateway.entry_point.storeInterfacer
        #light wrapper around ds interface in java, doesn't do too much
        #get rid of it, use datastore methods instead
        #use java datastore method instead

    """ Get Indeces from this datastore
    Returns:
        Index list: Index list of every index this java ref holds 
    """
    def get_indices(self):
        #j_indices = self.interfacer.getIndices(self._java_ref)
        j_indices = self.__java_ref.getIndices()
        return [Index(self._gateway, j_index) for j_index in j_indices]


    """ Adds type of adapter and index
    Args:
        type_adapter: A PyGwJ java Wrapper of a type adapter
        index [PyGwJavaWrapper]: A PyGw Java Wrapper of an index
    """
    def add_type(self, type_adapter, index):
        """NOTE: This is slightly different from java api. Currently does not support var-arg initial indices"""
        j_adapter = type_adapter._java_ref
        j_index = index._java_ref
        self.interfacer.addType(self._java_ref, j_adapter, j_index)

    """ A data writter for the given datastore
    Args:
        type_adapter [type_adapter_name]: Type of type adapter
    Returns:
        Write: A data writter

    """
    def create_writer(self, type_adapter_name):
        j_writer = self._java_ref.createWriter(type_adapter_name)
        return Writer(self._gateway, j_writer)

class DataTypeAdapter(PyGwJavaWrapper):
    """Wrapper to expose all of DataTypeAdapter API
    Todo:
        * Implement API
    """

    """
     name of this Adapter
    Returns:
        String: Name of Adapter
    """
    def get_type_name(self):
        return self._java_ref.getTypeName()

class Index(PyGwJavaWrapper):
    """Wrapper to expose all of Index API"""
    """
     name of this Index
    Returns:
        String: Name of Index
    """
    def get_name(self):
        return self._java_ref.getName()
    
    """
     Strategy name of this Index
    Returns:
        String: Index strategy
    """
    def get_index_strategy(self):
        j_obj = self._java_ref.getIndexStrategy()
        return j_obj.getClass().toString()

    """
    Model name of this Index
    Returns:
        String: Index model
    """
    def get_index_model(self):
        j_obj = self._java_ref.getIndexModel()
        return j_obj.getClass().toString()

class Writer(PyGwJavaWrapper):
    """Wrapper to expose all of Writer API"""
    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)
        self.is_open = True

    """
    Writes data with this writer
    Args:
        String: data
    Raises:
        RuntimeError: 'Writer is already closed' If writer is closed
    """
    def write(self, data):
        if not self.is_open:
            raise RuntimeError("Writer is already closed!")
        self._java_ref.write(data)
    
    """ closes this writer"""
    def close(self):
        if self.is_open:
            self._java_ref.close()
            self.is_open = False
    # Might want to introduce a method/flag here that bulk writes in java.
    # Ex. give it a list of a data and it calls a java endpoint to do all the writing and closing.
    # Current pipeline will make N py4j calls for an N-element ingest