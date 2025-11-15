"""
多源数据集成底座模块

该模块提供统一的数据上传、预处理、存储和检索功能，为多模态大模型医学病理诊断系统提供数据支撑。
"""

# 版本信息
__version__ = '1.0.0'
__author__ = 'Medical AI Team'
__description__ = '多源数据集成底座 - 支持医学图像、病历、检验结果等多模态数据的统一管理'

# 从各子模块导入核心类和函数
# 数据上传模块
from .uploaders import (
    ImageUploader, SystemIntegration, BatchUploadManager,
    BatchUploadTask, get_image_uploader, get_system_integration,
    get_batch_manager, supported_image_formats, supported_system_types
)

# 数据预处理模块
from .preprocessors import (
    ImageQualityEnhancer, TextDataProcessor, TimeSeriesProcessor,
    preprocess_image, preprocess_text, preprocess_time_series,
    enhance_image_quality, normalize_color, virtual_staining
)

# 数据存储与检索模块
from .storage import (
    ImageStorageManager, DocumentStorageManager, MetadataIndexManager,
    MultiModalRetrievalEngine, StorageFactory,
    create_default_storage_pipeline, get_storage_manager_by_type
)

# 统一导出列表
__all__ = [
    # 数据上传
    'ImageUploader', 'SystemIntegration', 'BatchUploadManager',
    'BatchUploadTask', 'get_image_uploader', 'get_system_integration',
    'get_batch_manager', 'supported_image_formats', 'supported_system_types',
    
    # 数据预处理
    'ImageQualityEnhancer', 'TextDataProcessor', 'TimeSeriesProcessor',
    'preprocess_image', 'preprocess_text', 'preprocess_time_series',
    'enhance_image_quality', 'normalize_color', 'virtual_staining',
    
    # 数据存储与检索
    'ImageStorageManager', 'DocumentStorageManager', 'MetadataIndexManager',
    'MultiModalRetrievalEngine', 'StorageFactory',
    'create_default_storage_pipeline', 'get_storage_manager_by_type',
    
    # 版本信息
    '__version__', '__author__', '__description__'
]

# 模块初始化函数
def initialize(config=None):
    """
    初始化数据集成模块
    
    Args:
        config: 配置参数字典
        
    Returns:
        初始化后的组件字典
    """
    components = {}
    
    # 初始化存储工厂和组件
    if config and 'storage' in config:
        factory = StorageFactory(config['storage'])
    else:
        factory = StorageFactory()
    
    components['storage_factory'] = factory
    components['storage_pipeline'] = factory.create_storage_pipeline()
    
    # 初始化上传管理器
    if config and 'upload' in config:
        components['image_uploader'] = get_image_uploader(config['upload'])
        components['system_integration'] = get_system_integration(config['upload'])
        components['batch_manager'] = get_batch_manager(config['upload'])
    else:
        components['image_uploader'] = get_image_uploader()
        components['system_integration'] = get_system_integration()
        components['batch_manager'] = get_batch_manager()
    
    # 初始化预处理组件
    components['image_preprocessor'] = ImageQualityEnhancer()
    components['text_preprocessor'] = TextDataProcessor()
    components['time_series_processor'] = TimeSeriesProcessor()
    
    return components

# 提供一个简单的API类来封装常用功能
class DataIntegrationAPI:
    """
    数据集成模块的高级API接口
    封装常用的数据处理、存储和检索功能
    """
    
    def __init__(self, config=None):
        """
        初始化API接口
        
        Args:
            config: 配置参数字典
        """
        self.components = initialize(config)
        
        # 便捷访问核心组件
        self.image_uploader = self.components['image_uploader']
        self.system_integration = self.components['system_integration']
        self.batch_manager = self.components['batch_manager']
        self.image_preprocessor = self.components['image_preprocessor']
        self.text_preprocessor = self.components['text_preprocessor']
        self.time_series_processor = self.components['time_series_processor']
        
        # 存储组件
        self.storage_pipeline = self.components['storage_pipeline']
        self.image_storage = self.storage_pipeline['image_storage']
        self.document_storage = self.storage_pipeline['document_storage']
        self.metadata_index = self.storage_pipeline['metadata_index']
        self.retrieval_engine = self.storage_pipeline['retrieval_engine']
    
    def upload_and_process_image(self, file_path, metadata=None):
        """
        上传并处理图像的一站式方法
        
        Args:
            file_path: 图像文件路径
            metadata: 图像元数据
            
        Returns:
            处理结果字典
        """
        # 验证文件
        validation_result = self.image_uploader.validate_file(file_path)
        if not validation_result['valid']:
            return {
                'success': False,
                'error': validation_result['error'],
                'step': 'validation'
            }
        
        # 预处理图像
        try:
            # 这里只是示例，实际预处理策略应根据需求调整
            processed_image = self.image_preprocessor.enhance_quality(file_path)
            preprocessed_path = self.image_preprocessor.save_processed_image(
                processed_image, file_path + '.processed'
            )
        except Exception as e:
            return {
                'success': False,
                'error': f'预处理失败: {str(e)}',
                'step': 'preprocessing'
            }
        
        # 存储图像
        try:
            storage_result = self.image_storage.store_image(
                preprocessed_path, metadata=metadata
            )
            return {
                'success': True,
                'step': 'storage',
                'data': storage_result
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'存储失败: {str(e)}',
                'step': 'storage'
            }
    
    def upload_and_process_document(self, file_path, metadata=None):
        """
        上传并处理文档的一站式方法
        
        Args:
            file_path: 文档文件路径
            metadata: 文档元数据
            
        Returns:
            处理结果字典
        """
        # 提取文本内容（如果是PDF或其他文本格式）
        try:
            # 这里应该添加适当的文档格式检测和内容提取
            # 简化示例，实际应用需要更复杂的处理
            if metadata is None:
                metadata = {}
            
            # 预处理文档（例如从PDF提取文本）
            if file_path.lower().endswith('.pdf'):
                # 这里添加PDF文本提取逻辑
                pass
                
        except Exception as e:
            return {
                'success': False,
                'error': f'文档预处理失败: {str(e)}',
                'step': 'preprocessing'
            }
        
        # 存储文档
        try:
            storage_result = self.document_storage.store_document(
                file_path, metadata=metadata
            )
            return {
                'success': True,
                'step': 'storage',
                'data': storage_result
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'存储失败: {str(e)}',
                'step': 'storage'
            }
    
    def search_multimodal(self, query, modalities=None, filters=None):
        """
        执行多模态搜索
        
        Args:
            query: 搜索查询
            modalities: 模态过滤列表
            filters: 额外过滤条件
            
        Returns:
            搜索结果
        """
        return self.retrieval_engine.search(
            query=query,
            modalities=modalities,
            filters=filters
        )
    
    def get_patient_data(self, patient_id, modalities=None):
        """
        获取患者的所有数据
        
        Args:
            patient_id: 患者ID
            modalities: 模态过滤列表
            
        Returns:
            患者数据搜索结果
        """
        return self.retrieval_engine.search_by_patient(
            patient_id=patient_id,
            modalities=modalities
        )
    
    def update_metadata(self, entity_id, entity_type, metadata_updates):
        """
        更新实体的元数据
        
        Args:
            entity_id: 实体ID
            entity_type: 实体类型
            metadata_updates: 要更新的元数据字段
            
        Returns:
            更新结果
        """
        return self.metadata_index.update_metadata(
            entity_id=entity_id,
            entity_type=entity_type,
            metadata_updates=metadata_updates
        )
    
    def start_batch_upload(self, file_paths, metadata_list=None, priority='medium'):
        """
        启动批量上传任务
        
        Args:
            file_paths: 文件路径列表
            metadata_list: 对应文件的元数据列表
            priority: 任务优先级
            
        Returns:
            任务ID和状态
        """
        task = self.batch_manager.add_task(
            file_paths=file_paths,
            metadata_list=metadata_list,
            priority=priority
        )
        
        return {
            'task_id': task.task_id,
            'status': task.status,
            'total_files': len(file_paths),
            'created_at': task.created_at
        }
    
    def get_batch_task_status(self, task_id):
        """
        获取批量任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态信息
        """
        task = self.batch_manager.get_task(task_id)
        if not task:
            return {'success': False, 'error': '任务不存在'}
        
        return {
            'success': True,
            'task_id': task.task_id,
            'status': task.status,
            'progress': task.progress,
            'total_files': task.total_files,
            'completed_files': task.completed_files,
            'failed_files': task.failed_files,
            'created_at': task.created_at,
            'updated_at': task.updated_at
        }

# 创建默认API实例
_default_api = None

def get_api(config=None):
    """
    获取数据集成API实例（单例模式）
    
    Args:
        config: 配置参数字典
        
    Returns:
        DataIntegrationAPI实例
    """
    global _default_api
    if _default_api is None:
        _default_api = DataIntegrationAPI(config)
    return _default_api

# 模块级别的便捷函数
def search(query, modalities=None, filters=None):
    """
    便捷搜索函数
    """
    api = get_api()
    return api.search_multimodal(query, modalities, filters)

def upload_image(file_path, metadata=None):
    """
    便捷图像上传函数
    """
    api = get_api()
    return api.upload_and_process_image(file_path, metadata)

def upload_document(file_path, metadata=None):
    """
    便捷文档上传函数
    """
    api = get_api()
    return api.upload_and_process_document(file_path, metadata)

def get_patient(patient_id, modalities=None):
    """
    便捷获取患者数据函数
    """
    api = get_api()
    return api.get_patient_data(patient_id, modalities)