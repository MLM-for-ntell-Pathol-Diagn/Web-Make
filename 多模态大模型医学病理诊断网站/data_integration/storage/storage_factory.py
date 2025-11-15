"""
存储工厂模块

该模块提供统一的存储组件创建接口，负责初始化和配置各种存储管理器和检索引擎。
"""
import os
import json
import logging
from typing import Dict, Optional, Any

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageFactory:
    """
    存储工厂类
    负责创建和初始化各种存储组件，实现依赖注入和组件管理
    """
    
    # 默认配置
    DEFAULT_CONFIG = {
        'storage_base_path': '/data/medical_imaging',
        'index_path': '/data/indexes',
        'temp_path': '/tmp/medical_imaging',
        'image_storage': {
            'max_file_size_mb': 1000,  # 1GB
            'allowed_formats': ['.tif', '.tiff', '.svs', '.ndpi', '.jpg', '.jpeg', '.png'],
            'chunk_size': 8192,
            'enable_hash': True
        },
        'document_storage': {
            'max_file_size_mb': 50,
            'allowed_formats': ['.pdf', '.txt', '.docx', '.json', '.xml', '.csv'],
            'enable_fulltext_index': True
        },
        'metadata_index': {
            'batch_size': 1000,
            'refresh_interval_seconds': 30,
            'enable_caching': True
        },
        'retrieval_engine': {
            'default_limit': 50,
            'enable_ranking': True
        }
    }
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化存储工厂
        
        Args:
            config: 配置参数，如果为None则使用默认配置
        """
        # 合并配置
        self.config = self.DEFAULT_CONFIG.copy()
        if config:
            self._merge_config(self.config, config)
        
        # 创建必要的目录
        self._ensure_directories()
        
        # 存储已创建的组件
        self._components = {}
        
        logger.info("存储工厂初始化完成")
    
    def get_image_storage_manager(self, config: Optional[Dict] = None) -> 'ImageStorageManager':
        """
        获取图像存储管理器实例
        
        Args:
            config: 可选的配置覆盖
            
        Returns:
            ImageStorageManager实例
        """
        component_key = 'image_storage_manager'
        
        # 检查缓存
        if component_key in self._components:
            return self._components[component_key]
        
        # 合并配置
        merged_config = self.config['image_storage'].copy()
        if config:
            self._merge_config(merged_config, config)
        
        # 添加基础路径
        merged_config['base_path'] = os.path.join(
            self.config['storage_base_path'], 'images'
        )
        merged_config['temp_path'] = self.config['temp_path']
        
        # 导入类（避免循环导入）
        from .image_storage import ImageStorageManager
        
        # 创建实例
        manager = ImageStorageManager(merged_config)
        self._components[component_key] = manager
        
        logger.info("图像存储管理器创建完成")
        return manager
    
    def get_document_storage_manager(self, config: Optional[Dict] = None) -> 'DocumentStorageManager':
        """
        获取文档存储管理器实例
        
        Args:
            config: 可选的配置覆盖
            
        Returns:
            DocumentStorageManager实例
        """
        component_key = 'document_storage_manager'
        
        # 检查缓存
        if component_key in self._components:
            return self._components[component_key]
        
        # 合并配置
        merged_config = self.config['document_storage'].copy()
        if config:
            self._merge_config(merged_config, config)
        
        # 添加基础路径
        merged_config['base_path'] = os.path.join(
            self.config['storage_base_path'], 'documents'
        )
        merged_config['temp_path'] = self.config['temp_path']
        
        # 导入类（避免循环导入）
        from .document_storage import DocumentStorageManager
        
        # 创建实例
        manager = DocumentStorageManager(merged_config)
        self._components[component_key] = manager
        
        logger.info("文档存储管理器创建完成")
        return manager
    
    def get_metadata_index_manager(self, config: Optional[Dict] = None) -> 'MetadataIndexManager':
        """
        获取元数据索引管理器实例
        
        Args:
            config: 可选的配置覆盖
            
        Returns:
            MetadataIndexManager实例
        """
        component_key = 'metadata_index_manager'
        
        # 检查缓存
        if component_key in self._components:
            return self._components[component_key]
        
        # 合并配置
        merged_config = self.config['metadata_index'].copy()
        if config:
            self._merge_config(merged_config, config)
        
        # 添加索引路径
        merged_config['index_path'] = self.config['index_path']
        
        # 导入类（避免循环导入）
        from .metadata_index import MetadataIndexManager
        
        # 创建实例
        manager = MetadataIndexManager(merged_config)
        self._components[component_key] = manager
        
        logger.info("元数据索引管理器创建完成")
        return manager
    
    def get_retrieval_engine(self, config: Optional[Dict] = None) -> 'MultiModalRetrievalEngine':
        """
        获取多模态检索引擎实例
        
        Args:
            config: 可选的配置覆盖
            
        Returns:
            MultiModalRetrievalEngine实例
        """
        component_key = 'retrieval_engine'
        
        # 检查缓存
        if component_key in self._components:
            return self._components[component_key]
        
        # 合并配置
        merged_config = self.config['retrieval_engine'].copy()
        if config:
            self._merge_config(merged_config, config)
        
        # 添加依赖组件
        merged_config['metadata_index_manager'] = self.get_metadata_index_manager()
        merged_config['image_storage_manager'] = self.get_image_storage_manager()
        merged_config['document_storage_manager'] = self.get_document_storage_manager()
        
        # 导入类（避免循环导入）
        from .retrieval_engine import MultiModalRetrievalEngine
        
        # 创建实例
        engine = MultiModalRetrievalEngine(merged_config)
        self._components[component_key] = engine
        
        logger.info("多模态检索引擎创建完成")
        return engine
    
    def create_storage_pipeline(self, config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        创建完整的存储处理管道
        
        Args:
            config: 可选的配置覆盖
            
        Returns:
            包含所有组件的字典
        """
        # 如果提供了新配置，重新初始化工厂
        if config:
            factory = StorageFactory(config)
        else:
            factory = self
        
        # 创建所有组件
        pipeline = {
            'image_storage': factory.get_image_storage_manager(),
            'document_storage': factory.get_document_storage_manager(),
            'metadata_index': factory.get_metadata_index_manager(),
            'retrieval_engine': factory.get_retrieval_engine(),
            'factory': factory  # 包含工厂自身以便访问
        }
        
        logger.info("存储处理管道创建完成")
        return pipeline
    
    def reset_cache(self, component_name: Optional[str] = None):
        """
        重置组件缓存
        
        Args:
            component_name: 要重置的组件名称，如果为None则重置所有组件
        """
        if component_name:
            if component_name in self._components:
                del self._components[component_name]
                logger.info(f"组件 {component_name} 缓存已重置")
            else:
                logger.warning(f"未找到组件 {component_name}")
        else:
            self._components.clear()
            logger.info("所有组件缓存已重置")
    
    def get_component(self, component_name: str) -> Optional[Any]:
        """
        获取已创建的组件
        
        Args:
            component_name: 组件名称
            
        Returns:
            组件实例，如果不存在则返回None
        """
        return self._components.get(component_name)
    
    def update_config(self, new_config: Dict):
        """
        更新配置
        
        Args:
            new_config: 新的配置参数
        """
        self._merge_config(self.config, new_config)
        self._ensure_directories()
        
        # 重置缓存以应用新配置
        self.reset_cache()
        
        logger.info("配置已更新，所有组件缓存已重置")
    
    def get_config(self) -> Dict:
        """
        获取当前配置
        
        Returns:
            配置字典
        """
        return self.config.copy()
    
    def _ensure_directories(self):
        """
        确保必要的目录存在
        """
        directories = [
            self.config['storage_base_path'],
            os.path.join(self.config['storage_base_path'], 'images'),
            os.path.join(self.config['storage_base_path'], 'documents'),
            self.config['index_path'],
            self.config['temp_path']
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.info(f"创建目录: {directory}")
                except Exception as e:
                    logger.error(f"创建目录失败 {directory}: {e}")
                    raise
    
    def _merge_config(self, base: Dict, update: Dict):
        """
        递归合并配置字典
        
        Args:
            base: 基础配置
            update: 要合并的配置
        """
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                # 递归合并嵌套字典
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def validate_config(self) -> bool:
        """
        验证配置是否有效
        
        Returns:
            配置是否有效的布尔值
        """
        try:
            # 验证必要的配置项
            required_keys = ['storage_base_path', 'index_path', 'temp_path']
            for key in required_keys:
                if key not in self.config:
                    logger.error(f"缺少必要的配置项: {key}")
                    return False
                
                # 验证路径格式
                path = self.config[key]
                if not isinstance(path, str) or not path.strip():
                    logger.error(f"配置项 {key} 必须是非空字符串")
                    return False
            
            # 验证存储配置
            for storage_type in ['image_storage', 'document_storage']:
                if storage_type not in self.config:
                    logger.error(f"缺少存储配置: {storage_type}")
                    return False
                
                storage_config = self.config[storage_type]
                # 验证最大文件大小
                if 'max_file_size_mb' not in storage_config:
                    logger.error(f"{storage_type} 缺少 max_file_size_mb 配置")
                    return False
                
                if not isinstance(storage_config['max_file_size_mb'], (int, float)) or storage_config['max_file_size_mb'] <= 0:
                    logger.error(f"{storage_type} 的 max_file_size_mb 必须是大于0的数字")
                    return False
                
                # 验证允许的格式
                if 'allowed_formats' not in storage_config:
                    logger.error(f"{storage_type} 缺少 allowed_formats 配置")
                    return False
                
                if not isinstance(storage_config['allowed_formats'], list) or not storage_config['allowed_formats']:
                    logger.error(f"{storage_type} 的 allowed_formats 必须是非空列表")
                    return False
            
            logger.info("配置验证通过")
            return True
            
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            return False
    
    def export_config(self, file_path: str):
        """
        导出配置到文件
        
        Args:
            file_path: 导出文件路径
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            logger.info(f"配置已导出到: {file_path}")
        except Exception as e:
            logger.error(f"导出配置失败: {e}")
            raise
    
    @classmethod
    def from_config_file(cls, file_path: str) -> 'StorageFactory':
        """
        从配置文件创建存储工厂
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            StorageFactory实例
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"从文件加载配置: {file_path}")
            return cls(config)
        except Exception as e:
            logger.error(f"从文件加载配置失败 {file_path}: {e}")
            raise
    
    def __str__(self) -> str:
        """
        返回工厂的字符串表示
        """
        component_count = len(self._components)
        config_status = "有效" if self.validate_config() else "无效"
        return f"StorageFactory(组件数={component_count}, 配置状态={config_status})"
    
    def __repr__(self) -> str:
        """
        返回工厂的详细字符串表示
        """
        return self.__str__()


def create_default_storage_pipeline() -> Dict[str, Any]:
    """
    创建默认的存储处理管道
    
    Returns:
        包含所有组件的字典
    """
    factory = StorageFactory()
    return factory.create_storage_pipeline()

def get_storage_manager_by_type(storage_type: str, 
                              factory: Optional[StorageFactory] = None) -> Any:
    """
    根据类型获取存储管理器
    
    Args:
        storage_type: 存储类型 ('image', 'document', 'metadata', 'retrieval')
        factory: 存储工厂实例，如果为None则创建新实例
        
    Returns:
        对应的存储管理器实例
    """
    if factory is None:
        factory = StorageFactory()
    
    managers = {
        'image': factory.get_image_storage_manager,
        'document': factory.get_document_storage_manager,
        'metadata': factory.get_metadata_index_manager,
        'retrieval': factory.get_retrieval_engine
    }
    
    if storage_type not in managers:
        raise ValueError(f"不支持的存储类型: {storage_type}")
    
    return managers[storage_type]()