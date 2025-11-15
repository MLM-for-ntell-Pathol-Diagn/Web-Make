"""
数据集成模块配置文件

该模块提供数据集成底座的配置管理功能，支持多环境配置切换和参数验证。
"""
import os
import json
import logging
from typing import Dict, Optional, Any
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigManager:
    """
    配置管理器类
    负责加载、验证和提供配置参数
    """
    
    # 默认配置
    DEFAULT_CONFIG = {
        # 基础配置
        'app_name': '多源数据集成底座',
        'version': '1.0.0',
        'debug': True,
        'log_level': 'INFO',
        
        # 存储配置
        'storage': {
            'storage_base_path': os.path.join(str(Path.home()), 'medical_data'),
            'index_path': os.path.join(str(Path.home()), 'medical_indexes'),
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
        },
        
        # 上传配置
        'upload': {
            'max_concurrent_uploads': 5,
            'max_queue_size': 100,
            'upload_timeout_seconds': 3600,  # 1小时
            'temp_upload_path': '/tmp/medical_uploads',
            'image_upload': {
                'chunk_size_bytes': 8388608,  # 8MB
                'max_total_size_mb': 5000,  # 5GB
                'enable_compression': False
            },
            'system_integration': {
                'connection_timeout_seconds': 30,
                'retry_count': 3,
                'retry_delay_seconds': 5
            },
            'batch_processing': {
                'thread_pool_size': 4,
                'task_polling_interval_seconds': 2
            }
        },
        
        # 预处理配置
        'preprocessing': {
            'image_quality': {
                'denoise_strength': 0.1,
                'resize_keep_ratio': True,
                'default_quality': 95
            },
            'text_processing': {
                'min_term_length': 2,
                'enable_stemming': False,
                'enable_lemmatization': True
            },
            'time_series': {
                'resample_frequency': '5T',  # 5分钟
                'max_gap_minutes': 30
            }
        },
        
        # Flask集成配置
        'flask': {
            'secret_key': 'dev_key_change_in_production',
            'max_content_length_mb': 100,
            'temp_dir': '/tmp/medical_flask',
            'allowed_origins': ['*']
        },
        
        # 医疗系统集成配置
        'medical_systems': {
            'his': {
                'enabled': False,
                'base_url': 'http://his.example.com/api',
                'auth_type': 'basic',
                'username': '',
                'password': ''
            },
            'emr': {
                'enabled': False,
                'base_url': 'http://emr.example.com/api',
                'auth_type': 'token',
                'api_key': ''
            },
            'pacs': {
                'enabled': False,
                'base_url': 'http://pacs.example.com/dcm4chee-arc/aets/DCM4CHEE/rs',
                'auth_type': 'basic',
                'username': '',
                'password': ''
            },
            'lis': {
                'enabled': False,
                'base_url': 'http://lis.example.com/api',
                'auth_type': 'basic',
                'username': '',
                'password': ''
            }
        }
    }
    
    # 环境变量映射
    ENV_VAR_MAPPINGS = {
        'storage_base_path': 'MEDICAL_STORAGE_PATH',
        'temp_path': 'MEDICAL_TEMP_PATH',
        'debug': 'MEDICAL_DEBUG',
        'log_level': 'MEDICAL_LOG_LEVEL',
        'secret_key': 'MEDICAL_SECRET_KEY'
    }
    
    def __init__(self, config_file: Optional[str] = None, environment: str = 'development'):
        """
        初始化配置管理器
        
        Args:
            config_file: 配置文件路径
            environment: 环境名称 ('development', 'testing', 'production')
        """
        self.environment = environment
        self.config = self.DEFAULT_CONFIG.copy()
        
        # 从配置文件加载
        if config_file:
            self._load_from_file(config_file)
        
        # 从环境变量加载
        self._load_from_env()
        
        # 根据环境调整配置
        self._adjust_for_environment()
        
        # 验证配置
        self._validate_config()
        
        # 创建必要的目录
        self._ensure_directories()
        
        logger.info(f"配置管理器初始化完成，环境: {environment}")
    
    def _load_from_file(self, config_file: str):
        """
        从配置文件加载配置
        
        Args:
            config_file: 配置文件路径
        """
        try:
            if not os.path.exists(config_file):
                logger.warning(f"配置文件不存在: {config_file}，使用默认配置")
                return
            
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            
            # 合并配置
            self._merge_config(self.config, file_config)
            logger.info(f"从文件加载配置: {config_file}")
            
        except json.JSONDecodeError as e:
            logger.error(f"解析配置文件失败 {config_file}: {e}")
            raise
        except Exception as e:
            logger.error(f"加载配置文件失败 {config_file}: {e}")
            raise
    
    def _load_from_env(self):
        """
        从环境变量加载配置
        """
        for config_key, env_key in self.ENV_VAR_MAPPINGS.items():
            env_value = os.environ.get(env_key)
            if env_value is not None:
                # 根据配置键的嵌套结构设置值
                if '.' in config_key:
                    parts = config_key.split('.')
                    target = self.config
                    for part in parts[:-1]:
                        if part not in target:
                            target[part] = {}
                        target = target[part]
                    target[parts[-1]] = self._parse_env_value(env_value)
                else:
                    self.config[config_key] = self._parse_env_value(env_value)
                
                logger.info(f"从环境变量加载配置: {env_key} -> {config_key}")
    
    def _parse_env_value(self, value: str):
        """
        解析环境变量值到适当的类型
        
        Args:
            value: 环境变量值
            
        Returns:
            解析后的值
        """
        # 尝试解析为布尔值
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # 尝试解析为整数
        try:
            return int(value)
        except ValueError:
            pass
        
        # 尝试解析为浮点数
        try:
            return float(value)
        except ValueError:
            pass
        
        # 尝试解析为列表（逗号分隔）
        if ',' in value:
            return [v.strip() for v in value.split(',')]
        
        # 默认返回字符串
        return value
    
    def _adjust_for_environment(self):
        """
        根据环境调整配置
        """
        if self.environment == 'production':
            # 生产环境配置调整
            self.config['debug'] = False
            if self.config['log_level'] == 'DEBUG':
                self.config['log_level'] = 'INFO'
            
            # 增强安全性
            if self.config['flask']['secret_key'] == 'dev_key_change_in_production':
                logger.warning("生产环境使用默认密钥，强烈建议更改!")
            
            # 调整并发设置
            self.config['upload']['max_concurrent_uploads'] = 10
            self.config['upload']['batch_processing']['thread_pool_size'] = 8
            
        elif self.environment == 'testing':
            # 测试环境配置调整
            self.config['debug'] = True
            self.config['log_level'] = 'DEBUG'
            
            # 使用测试专用目录
            self.config['storage']['storage_base_path'] = '/tmp/medical_test_storage'
            self.config['storage']['index_path'] = '/tmp/medical_test_indexes'
            
        # 设置日志级别
        numeric_level = getattr(logging, self.config['log_level'].upper(), logging.INFO)
        logging.getLogger().setLevel(numeric_level)
    
    def _validate_config(self):
        """
        验证配置的有效性
        """
        try:
            # 验证存储配置
            storage_config = self.config['storage']
            
            # 验证基础路径
            for path_key in ['storage_base_path', 'index_path', 'temp_path']:
                if path_key not in storage_config:
                    logger.error(f"缺少必要的存储配置: {path_key}")
                    raise ValueError(f"配置错误: 缺少 {path_key}")
            
            # 验证文件大小限制
            for storage_type in ['image_storage', 'document_storage']:
                if storage_type in storage_config:
                    if 'max_file_size_mb' in storage_config[storage_type]:
                        size = storage_config[storage_type]['max_file_size_mb']
                        if size <= 0:
                            logger.error(f"文件大小限制必须大于0: {storage_type}")
                            raise ValueError(f"配置错误: {storage_type} 文件大小限制无效")
            
            # 验证上传配置
            upload_config = self.config['upload']
            if upload_config['max_concurrent_uploads'] <= 0:
                logger.error("最大并发上传数必须大于0")
                raise ValueError("配置错误: 最大并发上传数无效")
            
            logger.info("配置验证通过")
            
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            raise
    
    def _ensure_directories(self):
        """
        确保必要的目录存在
        """
        storage_config = self.config['storage']
        directories = [
            storage_config['storage_base_path'],
            os.path.join(storage_config['storage_base_path'], 'images'),
            os.path.join(storage_config['storage_base_path'], 'documents'),
            storage_config['index_path'],
            storage_config['temp_path'],
            self.config['upload']['temp_upload_path']
        ]
        
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"确保目录存在: {directory}")
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
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键（支持点表示法，如'storage.image_storage.max_file_size_mb'）
            default: 默认值
            
        Returns:
            配置值或默认值
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any):
        """
        设置配置值
        
        Args:
            key: 配置键（支持点表示法）
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        
        # 导航到目标位置
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            elif not isinstance(config[k], dict):
                config[k] = {}
            config = config[k]
        
        # 设置值
        config[keys[-1]] = value
        logger.info(f"更新配置: {key} = {value}")
    
    def get_storage_config(self) -> Dict:
        """
        获取存储配置
        
        Returns:
            存储配置字典
        """
        return self.config['storage'].copy()
    
    def get_upload_config(self) -> Dict:
        """
        获取上传配置
        
        Returns:
            上传配置字典
        """
        return self.config['upload'].copy()
    
    def get_preprocessing_config(self) -> Dict:
        """
        获取预处理配置
        
        Returns:
            预处理配置字典
        """
        return self.config['preprocessing'].copy()
    
    def get_flask_config(self) -> Dict:
        """
        获取Flask集成配置
        
        Returns:
            Flask配置字典
        """
        return self.config['flask'].copy()
    
    def get_medical_system_config(self, system_type: str) -> Optional[Dict]:
        """
        获取医疗系统配置
        
        Args:
            system_type: 系统类型 ('his', 'emr', 'pacs', 'lis')
            
        Returns:
            系统配置字典，如果不存在则返回None
        """
        return self.config['medical_systems'].get(system_type)
    
    def export(self, file_path: str):
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
    
    def is_production(self) -> bool:
        """
        检查是否为生产环境
        
        Returns:
            是否为生产环境的布尔值
        """
        return self.environment == 'production'
    
    def is_development(self) -> bool:
        """
        检查是否为开发环境
        
        Returns:
            是否为开发环境的布尔值
        """
        return self.environment == 'development'
    
    def is_testing(self) -> bool:
        """
        检查是否为测试环境
        
        Returns:
            是否为测试环境的布尔值
        """
        return self.environment == 'testing'
    
    def __str__(self) -> str:
        """
        返回配置管理器的字符串表示
        """
        return f"ConfigManager(环境={self.environment}, 调试模式={self.config['debug']})"
    
    def __repr__(self) -> str:
        """
        返回配置管理器的详细字符串表示
        """
        return self.__str__()


# 全局配置实例
_config_manager = None

def get_config(environment: str = 'development') -> ConfigManager:
    """
    获取全局配置管理器实例（单例模式）
    
    Args:
        environment: 环境名称
        
    Returns:
        ConfigManager实例
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(environment=environment)
    return _config_manager

def load_config_from_file(config_file: str, environment: str = 'development') -> ConfigManager:
    """
    从文件加载配置
    
    Args:
        config_file: 配置文件路径
        environment: 环境名称
        
    Returns:
        ConfigManager实例
    """
    global _config_manager
    _config_manager = ConfigManager(config_file=config_file, environment=environment)
    return _config_manager

def create_default_config_file(file_path: str):
    """
    创建默认配置文件
    
    Args:
        file_path: 配置文件路径
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(ConfigManager.DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        logger.info(f"默认配置文件已创建: {file_path}")
    except Exception as e:
        logger.error(f"创建默认配置文件失败: {e}")
        raise


# 配置帮助函数
def get_data_integration_config(environment: str = 'development') -> Dict:
    """
    获取数据集成模块的配置字典
    
    Args:
        environment: 环境名称
        
    Returns:
        配置字典
    """
    config = get_config(environment)
    return {
        'storage': config.get_storage_config(),
        'upload': config.get_upload_config()
    }

def get_flask_app_config(environment: str = 'development') -> Dict:
    """
    获取Flask应用配置
    
    Args:
        environment: 环境名称
        
    Returns:
        Flask应用配置字典
    """
    config = get_config(environment)
    flask_config = config.get_flask_config()
    
    # 转换为Flask配置格式
    return {
        'SECRET_KEY': flask_config['secret_key'],
        'MAX_CONTENT_LENGTH': flask_config['max_content_length_mb'] * 1024 * 1024,
        'TEMP_DIR': flask_config['temp_dir'],
        'DEBUG': config.is_development()
    }


# 命令行函数
def main():
    """
    命令行入口，用于创建默认配置文件
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据集成模块配置工具')
    parser.add_argument('--create-config', type=str, help='创建默认配置文件')
    parser.add_argument('--environment', type=str, default='development', 
                      choices=['development', 'testing', 'production'],
                      help='环境配置')
    
    args = parser.parse_args()
    
    if args.create_config:
        create_default_config_file(args.create_config)
        print(f"默认配置文件已创建: {args.create_config}")
    else:
        # 显示当前配置
        config = get_config(args.environment)
        print(f"当前环境: {args.environment}")
        print(f"调试模式: {config.is_development()}")
        print(f"存储路径: {config.get('storage.storage_base_path')}")


if __name__ == '__main__':
    main()