"""
医学图像存储管理模块

该模块负责医学图像数据（如病理切片、医学影像等）的存储、检索和管理。
"""
import os
import json
import uuid
import logging
import hashlib
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
import numpy as np

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ImageStorageManager:
    """
    医学图像存储管理器
    提供图像数据的存储、检索、更新和删除功能
    """
    
    def __init__(self, config: Dict):
        """
        初始化图像存储管理器
        
        Args:
            config: 配置参数，包含以下关键字段：
                - storage_path: 图像存储根路径
                - metadata_path: 元数据存储路径
                - supported_formats: 支持的图像格式列表
                - enable_compression: 是否启用压缩
        """
        self.config = config
        self.storage_path = config.get('storage_path', './data/images')
        self.metadata_path = config.get('metadata_path', './data/metadata')
        self.supported_formats = config.get('supported_formats', ['.tif', '.tiff', '.jpg', '.jpeg', '.png', '.svs', '.ndpi'])
        self.enable_compression = config.get('enable_compression', False)
        
        # 创建必要的目录
        os.makedirs(self.storage_path, exist_ok=True)
        os.makedirs(self.metadata_path, exist_ok=True)
        
        logger.info(f"图像存储管理器初始化完成，存储路径: {self.storage_path}")
    
    def store_image(self, image_data: bytes, metadata: Dict, format_type: str = 'tif') -> str:
        """
        存储图像数据
        
        Args:
            image_data: 图像二进制数据
            metadata: 图像元数据
            format_type: 图像格式类型
            
        Returns:
            图像的唯一标识符（UUID）
        """
        # 生成唯一标识符
        image_id = str(uuid.uuid4())
        
        # 验证格式
        if f".{format_type}" not in self.supported_formats:
            raise ValueError(f"不支持的图像格式: {format_type}")
        
        # 计算文件哈希值以检查重复
        file_hash = hashlib.sha256(image_data).hexdigest()
        
        # 创建存储子目录（基于ID的前两位，避免单目录文件过多）
        sub_dir = image_id[:2]
        image_dir = os.path.join(self.storage_path, sub_dir)
        os.makedirs(image_dir, exist_ok=True)
        
        # 保存图像文件
        file_name = f"{image_id}.{format_type}"
        file_path = os.path.join(image_dir, file_name)
        
        try:
            with open(file_path, 'wb') as f:
                f.write(image_data)
            
            # 准备元数据
            full_metadata = {
                'id': image_id,
                'file_name': file_name,
                'file_path': file_path,
                'format': format_type,
                'size_bytes': len(image_data),
                'hash': file_hash,
                'storage_date': datetime.now().isoformat(),
                'compression': self.enable_compression,
                'metadata': metadata
            }
            
            # 保存元数据
            self._save_metadata(image_id, full_metadata)
            
            logger.info(f"图像 {image_id} 存储成功")
            return image_id
            
        except Exception as e:
            logger.error(f"存储图像失败: {e}")
            # 如果保存失败，尝试清理已创建的文件
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise
    
    def retrieve_image(self, image_id: str) -> Dict:
        """
        检索图像数据
        
        Args:
            image_id: 图像唯一标识符
            
        Returns:
            包含图像数据和元数据的字典
        """
        # 验证ID格式
        try:
            uuid.UUID(image_id)
        except ValueError:
            raise ValueError("无效的图像ID格式")
        
        # 获取元数据
        metadata = self.get_metadata(image_id)
        if not metadata:
            raise FileNotFoundError(f"未找到ID为 {image_id} 的图像")
        
        # 读取图像文件
        file_path = metadata.get('file_path')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"图像文件不存在: {file_path}")
        
        try:
            with open(file_path, 'rb') as f:
                image_data = f.read()
            
            result = {
                'id': image_id,
                'data': image_data,
                'metadata': metadata
            }
            
            return result
            
        except Exception as e:
            logger.error(f"检索图像失败: {e}")
            raise
    
    def get_metadata(self, image_id: str) -> Optional[Dict]:
        """
        获取图像元数据
        
        Args:
            image_id: 图像唯一标识符
            
        Returns:
            元数据字典，如果不存在返回None
        """
        metadata_file = self._get_metadata_file_path(image_id)
        
        if not os.path.exists(metadata_file):
            return None
        
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            return metadata
        except Exception as e:
            logger.error(f"读取元数据失败: {e}")
            return None
    
    def update_metadata(self, image_id: str, new_metadata: Dict) -> bool:
        """
        更新图像元数据
        
        Args:
            image_id: 图像唯一标识符
            new_metadata: 要更新的元数据
            
        Returns:
            更新是否成功
        """
        # 获取现有元数据
        existing_metadata = self.get_metadata(image_id)
        if not existing_metadata:
            logger.error(f"未找到ID为 {image_id} 的图像元数据")
            return False
        
        # 更新元数据
        existing_metadata['metadata'].update(new_metadata)
        existing_metadata['last_modified'] = datetime.now().isoformat()
        
        # 保存更新后的元数据
        try:
            self._save_metadata(image_id, existing_metadata)
            logger.info(f"图像 {image_id} 的元数据更新成功")
            return True
        except Exception as e:
            logger.error(f"更新元数据失败: {e}")
            return False
    
    def delete_image(self, image_id: str) -> bool:
        """
        删除图像及其元数据
        
        Args:
            image_id: 图像唯一标识符
            
        Returns:
            删除是否成功
        """
        # 获取元数据以找到文件路径
        metadata = self.get_metadata(image_id)
        if not metadata:
            logger.error(f"未找到ID为 {image_id} 的图像元数据")
            return False
        
        # 删除图像文件
        file_path = metadata.get('file_path')
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"删除图像文件失败: {e}")
                return False
        
        # 删除元数据文件
        metadata_file = self._get_metadata_file_path(image_id)
        if os.path.exists(metadata_file):
            try:
                os.remove(metadata_file)
            except Exception as e:
                logger.error(f"删除元数据文件失败: {e}")
                return False
        
        logger.info(f"图像 {image_id} 删除成功")
        return True
    
    def search_by_metadata(self, query: Dict, limit: int = 100) -> List[Dict]:
        """
        根据元数据搜索图像
        
        Args:
            query: 查询条件，支持简单的键值对匹配
            limit: 结果数量限制
            
        Returns:
            匹配的图像元数据列表
        """
        results = []
        
        # 遍历所有元数据文件
        for sub_dir in os.listdir(self.storage_path):
            sub_dir_path = os.path.join(self.storage_path, sub_dir)
            if not os.path.isdir(sub_dir_path):
                continue
            
            for file_name in os.listdir(sub_dir_path):
                # 跳过非图像文件
                _, ext = os.path.splitext(file_name)
                if ext.lower() not in self.supported_formats:
                    continue
                
                # 提取ID
                image_id = os.path.splitext(file_name)[0]
                
                # 获取元数据
                metadata = self.get_metadata(image_id)
                if not metadata:
                    continue
                
                # 检查是否匹配查询条件
                match = True
                for key, value in query.items():
                    if key not in metadata['metadata'] or metadata['metadata'][key] != value:
                        match = False
                        break
                
                if match:
                    results.append(metadata)
                    
                    # 检查是否达到限制
                    if len(results) >= limit:
                        return results
        
        return results
    
    def list_images(self, start_date: Optional[datetime] = None, 
                   end_date: Optional[datetime] = None, 
                   limit: int = 100) -> List[Dict]:
        """
        列出图像
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 结果数量限制
            
        Returns:
            图像元数据列表
        """
        results = []
        
        # 遍历所有元数据文件
        for sub_dir in os.listdir(self.storage_path):
            sub_dir_path = os.path.join(self.storage_path, sub_dir)
            if not os.path.isdir(sub_dir_path):
                continue
            
            for file_name in os.listdir(sub_dir_path):
                # 跳过非图像文件
                _, ext = os.path.splitext(file_name)
                if ext.lower() not in self.supported_formats:
                    continue
                
                # 提取ID
                image_id = os.path.splitext(file_name)[0]
                
                # 获取元数据
                metadata = self.get_metadata(image_id)
                if not metadata:
                    continue
                
                # 检查日期范围
                if start_date or end_date:
                    storage_date = datetime.fromisoformat(metadata['storage_date'])
                    if start_date and storage_date < start_date:
                        continue
                    if end_date and storage_date > end_date:
                        continue
                
                results.append(metadata)
                
                # 检查是否达到限制
                if len(results) >= limit:
                    return results
        
        return results
    
    def get_storage_statistics(self) -> Dict:
        """
        获取存储统计信息
        
        Returns:
            统计信息字典
        """
        stats = {
            'total_images': 0,
            'total_size_bytes': 0,
            'format_distribution': {},
            'monthly_distribution': {}
        }
        
        # 遍历所有图像文件
        for root, _, files in os.walk(self.storage_path):
            for file_name in files:
                _, ext = os.path.splitext(file_name)
                if ext.lower() in self.supported_formats:
                    file_path = os.path.join(root, file_name)
                    stats['total_images'] += 1
                    
                    # 累加文件大小
                    try:
                        file_size = os.path.getsize(file_path)
                        stats['total_size_bytes'] += file_size
                    except Exception as e:
                        logger.warning(f"无法获取文件大小: {e}")
                    
                    # 更新格式分布
                    format_type = ext.lower().lstrip('.')
                    stats['format_distribution'][format_type] = \
                        stats['format_distribution'].get(format_type, 0) + 1
                    
                    # 更新月度分布
                    image_id = os.path.splitext(file_name)[0]
                    metadata = self.get_metadata(image_id)
                    if metadata:
                        try:
                            storage_date = datetime.fromisoformat(metadata['storage_date'])
                            month_key = storage_date.strftime('%Y-%m')
                            stats['monthly_distribution'][month_key] = \
                                stats['monthly_distribution'].get(month_key, 0) + 1
                        except Exception as e:
                            logger.warning(f"解析存储日期失败: {e}")
        
        # 转换为人类可读的格式
        stats['total_size_human'] = self._human_readable_size(stats['total_size_bytes'])
        
        return stats
    
    def _save_metadata(self, image_id: str, metadata: Dict):
        """
        保存元数据到文件
        
        Args:
            image_id: 图像唯一标识符
            metadata: 元数据字典
        """
        metadata_file = self._get_metadata_file_path(image_id)
        
        # 确保目录存在
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    def _get_metadata_file_path(self, image_id: str) -> str:
        """
        获取元数据文件路径
        
        Args:
            image_id: 图像唯一标识符
            
        Returns:
            元数据文件路径
        """
        # 使用与图像相同的子目录结构
        sub_dir = image_id[:2]
        return os.path.join(self.metadata_path, sub_dir, f"{image_id}.json")
    
    def _human_readable_size(self, size_bytes: int) -> str:
        """
        将字节数转换为人类可读的格式
        
        Args:
            size_bytes: 字节数
            
        Returns:
            人类可读的大小字符串
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"