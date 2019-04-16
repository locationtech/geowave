from pygw.config import config
# These classes define methods for interfacing with the
# abstract Classes/ Interfaces as defined in the core.store.api
# No Java objects should be instantiated here.
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

    """
    Get the indices that have been used within this data store for a particular type. If data type
        name is null it will return all indices.
    Args:
        string [type_name]: Data type name
    Returns:
       Index List: An array of the indices for a given data type.

    """
    def get_indices(self, type_name=None):
        if type_name:
            j_indices = self._java_ref.getIndices(type_name)
        else:
            j_indices = self._java_ref.getIndices()
        return [Index(self._gateway, j_index) for j_index in j_indices]

    """
    Copy the subset of data matching this query from this store into a 
        specified other store, if query is null it will copy all data. 
    Args:
        string [type_name]: Data type name
    Returns:
       Index List: An array of the indices for a given data type.

    """
    def copy_to(self, ds, q=None):
        assert isinstance(ds, DataStore)

        return self._java_ref.copyTo(ds._java_ref,q)

    """
    Add new indices for the given type. If there is data in other indices for this type, for
    consistency it will need to copy all of the data into the new indices, which could be a long
    process for lots of data.

    Args:
        string [type_name]: Data type name
    Returns:
       Index List: the new indices to add.

    """
    def add_index(self, type_name, *indices):
        assert isinstance(type_name,str)

        n = len(indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, py_obj in enumerate(indices):
                j_index_arr[idx] = py_obj._java_ref
    
        self._java_ref.addType(type_name,j_index_arr)
    """
    Remove an index completely for a given type. If type_name is null, it will remove the 
    index for all types. If this is the last index for any type it throws an
    illegal state exception, expecting the user to remove the type before removing the index to
    protect a user from losing any reference to their data unknowingly for a type.

    Args:
        string [type_name]: the type name
        string [index_name]: the index name
    Throws:
       IllegalStateException: if this is the last index for a type, remove the type first

    """
    def remove_index(self, index_name, type_name=None):
        if type_name:
            return self._java_ref.removeIndex(index_name,type_name)
        else:
            return self._java_ref.removeIndex(index_name)

    """
    Remove statistics for type

    Args:
        string [type_name]: the type name
    """
    def remove_type(self, type_name):
        assert isinstance(str,type_name)

        return self._java_ref.removeType(type_name)
    
    """
    Delete all data in this data store that matches the query parameter.

    Args:
        query [q]: query the query criteria to use for deletion
    Returns:
        Bool: true on success
    """
    def delete(self, q):
        assert isinstance(q,QueryInterface)

        return self._java_ref.delete(q)

    """
    Delete ALL data and ALL metadata for this datastore. This is provided for convenience as a
    simple way to wipe a datastore cleanly, but don't be surprised if everything is gone.
    """
    def delete_all(self):
        
      return self._java_ref.deleteAll()

    """
    Add this type to the data store. This only needs to be called one time ever per type.

    Args:
        string [type_adapter]: the data type adapter for this type that is used to read and write GeoWave entries
        Index list [initial_indices]: the initial indexing for this type, in the future additional indices can be added
    """
    def add_type(self, type_adapter, *initial_indices):
        assert isinstance(type_adapter,DataTypeAdapter)

        n = len(initial_indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, py_obj in enumerate(initial_indices):
                j_index_arr[idx] = py_obj._java_ref
    
        self._java_ref.addType(type_adapter._java_ref,j_index_arr)

<<<<<<< HEAD
    """
    Returns an index writer to perform batched write operations for the given data type name. It
    assumes the type has already been used previously or added using addType and assumes one or
    more indices have been provided for this type.

    Args:
        string [type_name]: the type name
    Returns:
        writer: a writer which can be used to write entries into this datastore of the given type 
    """    
    def create_writer(self, type_name):
        j_writer = self._java_ref.createWriter(type_name)
=======
    """ A data writter for the given datastore
    Args:
        type_adapter [type_adapter_name]: Type of type adapter
    Returns:
        Write: A data writter

    """
    def create_writer(self, type_adapter_name):
        j_writer = self._java_ref.createWriter(type_adapter_name)
>>>>>>> bf22f8f368ef57e4352e5339da800a3a0180f5e4
        return Writer(self._gateway, j_writer)

    """
    Ingest from URL. If this is a directory, this method will recursively search for valid files to
    ingest in the directory. This will iterate through registered IngestFormatPlugins to find one
    that works for a given file. The applicable ingest format plugin will choose the
    DataTypeAdapter and may even use additional indices than the one provided.

    Args:
        string [url]: The URL for data to read and ingest into this data store
        Index List [indices]: The indexing approach to use.
    """ 
    def ingest(self, url, *indices, ingest_options=None):
        #TODO: Ingest Options

        assert isinstance(url,str)

        n = len(indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, name in enumerate(indices):
                j_index_arr[idx] = name._java_ref
        java_url = config.GATEWAY.jvm.java.net.URL(url)
        self._java_ref.ingest(java_url,ingest_options,j_index_arr)
    
    def query(self, q):
        assert isinstance(q, QueryInterface)
        j_query = q._java_ref
        return self._java_ref.query(j_query)

    def aggregate(self, q):
        # TODO
        raise NotImplementedError
    
    def get_types(self):
        j_adapter_arr = self._java_ref.getTypes()
        return [DataTypeAdapter(self._gateway, j_adpt) for j_adpt in j_adapter_arr]

    def query_statistics(self, q):
        # TODO
        raise NotImplementedError

    def aggregate_statistics(self, q):
        # TODO
        raise NotImplementedError


class DataTypeAdapter(PyGwJavaWrapper):
    """Wrapper to expose all of DataTypeAdapter API"""
 
    def get_type_name(self):
        return self._java_ref.getTypeName()

    # TODO: Are the rest of the API methods as defined in the Java interface
    # necessary to be wrapped in Python?

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

class QueryInterface(PyGwJavaWrapper):
    # TODO: Just describe its properties
    
    def describe(self):
        print("I'm a query! ... Please implement a better description for me...")
