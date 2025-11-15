"""
多模态数据存储与检索模块

该模块提供统一的数据存储接口，支持图像、文本、时间序列等多种模态数据的存储与检索功能。
"""

from .image_storage import ImageStorageManager
from .document_storage import DocumentStorageManager
from .metadata_index import MetadataIndexManager
from .retrieval_engine import MultiModalRetrievalEngine
from .storage_factory import StorageFactory

__all__ = [
    'ImageStorageManager',
    'DocumentStorageManager', 
    'MetadataIndexManager',
    'MultiModalRetrievalEngine',
    'StorageFactory'
]

# 版本信息
__version__ = "0.1.0"

# 模块描述
__description__ = "多模态医学数据存储与检索系统"