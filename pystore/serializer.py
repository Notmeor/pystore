
import pickle
import hashlib
import lz4.block


settings = {
    'compress': False
}


def compress(b):
    return lz4.block.compress(b, mode='fast')


def decompress(b):
    return lz4.block.decompress(b)


class Serializer:

    @staticmethod
    def serialize(obj):
        ret = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        ret = compress(ret)
        return ret

    @staticmethod
    def deserialize(b):
        ret = decompress(b)
        ret = pickle.loads(ret)
        return ret

    @classmethod
    def gen_md5(cls, b, value=False):
        bytes_ = cls.serialize(b)
        md5 = hashlib.md5(bytes_).hexdigest()
        if value:
            return md5, bytes_
        return md5

serializer = Serializer