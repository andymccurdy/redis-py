from ..helpers import nativestr


def _parse_info_args(args):
    """Convert INFO response args to a dict with string keys.

    Handles both RESP2 (flat list) and RESP3 (dict) responses.
    """
    if isinstance(args, dict):
        return {nativestr(k): v for k, v in args.items()}
    return dict(zip(map(nativestr, args[::2]), args[1::2]))


class BFInfo:
    capacity = None
    size = None
    filterNum = None
    insertedNum = None
    expansionRate = None

    def __init__(self, args):
        response = _parse_info_args(args)
        self.capacity = response["Capacity"]
        self.size = response["Size"]
        self.filterNum = response["Number of filters"]
        self.insertedNum = response["Number of items inserted"]
        self.expansionRate = response["Expansion rate"]

    def get(self, item):
        try:
            return self.__getitem__(item)
        except AttributeError:
            return None

    def __getitem__(self, item):
        return getattr(self, item)


class CFInfo:
    size = None
    bucketNum = None
    filterNum = None
    insertedNum = None
    deletedNum = None
    bucketSize = None
    expansionRate = None
    maxIteration = None

    def __init__(self, args):
        response = _parse_info_args(args)
        self.size = response["Size"]
        self.bucketNum = response["Number of buckets"]
        self.filterNum = response["Number of filters"]
        self.insertedNum = response["Number of items inserted"]
        self.deletedNum = response["Number of items deleted"]
        self.bucketSize = response["Bucket size"]
        self.expansionRate = response["Expansion rate"]
        self.maxIteration = response["Max iterations"]

    def get(self, item):
        try:
            return self.__getitem__(item)
        except AttributeError:
            return None

    def __getitem__(self, item):
        return getattr(self, item)


class CMSInfo:
    width = None
    depth = None
    count = None
    cell_size = None

    def __init__(self, args):
        response = _parse_info_args(args)
        self.width = response["width"]
        self.depth = response["depth"]
        self.count = response["count"]
        # Reported by RedisBloom versions that support CELL_SIZE; older servers
        # omit the field.
        self.cell_size = response.get("cell_size")

    def __getitem__(self, item):
        return getattr(self, item)


class TopKInfo:
    k = None
    width = None
    depth = None
    decay = None

    def __init__(self, args):
        response = _parse_info_args(args)
        self.k = response["k"]
        self.width = response["width"]
        self.depth = response["depth"]
        self.decay = response["decay"]

    def __getitem__(self, item):
        return getattr(self, item)


class TDigestInfo:
    compression = None
    capacity = None
    merged_nodes = None
    unmerged_nodes = None
    merged_weight = None
    unmerged_weight = None
    total_compressions = None
    memory_usage = None

    def __init__(self, args):
        response = _parse_info_args(args)
        self.compression = response["Compression"]
        self.capacity = response["Capacity"]
        self.merged_nodes = response["Merged nodes"]
        self.unmerged_nodes = response["Unmerged nodes"]
        self.merged_weight = response["Merged weight"]
        self.unmerged_weight = response["Unmerged weight"]
        self.total_compressions = response["Total compressions"]
        self.memory_usage = response["Memory usage"]

    def get(self, item):
        try:
            return self.__getitem__(item)
        except AttributeError:
            return None

    def __getitem__(self, item):
        return getattr(self, item)
