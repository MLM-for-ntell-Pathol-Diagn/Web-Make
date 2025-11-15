"""
元数据索引管理模块

该模块提供多模态医学数据的统一索引管理，支持跨模态数据的关联检索和语义搜索。
"""
import os
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, Optional, List, Any, Set, Union
import uuid

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataIndexManager:
    """
    元数据索引管理器
    提供统一的元数据索引服务，支持跨模态数据的索引、查询和关联
    """
    
    def __init__(self, config: Dict):
        """
        初始化元数据索引管理器
        
        Args:
            config: 配置参数，包含以下关键字段：
                - index_path: 索引存储路径
                - enable_caching: 是否启用缓存
                - cache_size: 缓存大小（条目数）
        """
        self.config = config
        self.index_path = config.get('index_path', './data/unified_index')
        self.enable_caching = config.get('enable_caching', True)
        self.cache_size = config.get('cache_size', 1000)
        
        # 创建索引目录
        os.makedirs(self.index_path, exist_ok=True)
        
        # 初始化缓存
        self.cache = {}
        self.cache_lru = []
        
        # 索引文件路径
        self.main_index_file = os.path.join(self.index_path, 'main_index.json')
        self.type_index_file = os.path.join(self.index_path, 'type_index.json')
        self.patient_index_file = os.path.join(self.index_path, 'patient_index.json')
        self.study_index_file = os.path.join(self.index_path, 'study_index.json')
        
        # 初始化索引文件
        self._initialize_index_files()
        
        logger.info(f"元数据索引管理器初始化完成，索引路径: {self.index_path}")
    
    def create_index(self, entity_id: str, entity_type: str, metadata: Dict, 
                    source_path: Optional[str] = None) -> bool:
        """
        为实体创建索引
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型（image, document, timeseries等）
            metadata: 实体元数据
            source_path: 源数据路径（可选）
            
        Returns:
            创建是否成功
        """
        try:
            # 创建索引条目
            index_entry = {
                'id': entity_id,
                'type': entity_type,
                'metadata': metadata,
                'source_path': source_path,
                'index_date': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            # 生成索引键
            index_key = f"{entity_type}:{entity_id}"
            
            # 加载主索引
            main_index = self._load_index(self.main_index_file)
            
            # 添加到主索引
            main_index[index_key] = index_entry
            
            # 保存主索引
            self._save_index(self.main_index_file, main_index)
            
            # 更新类型索引
            self._update_type_index(entity_type, entity_id)
            
            # 更新患者索引（如果有患者ID）
            patient_id = metadata.get('patient_id')
            if patient_id:
                self._update_patient_index(patient_id, entity_type, entity_id)
            
            # 更新研究索引（如果有研究ID）
            study_id = metadata.get('study_id')
            if study_id:
                self._update_study_index(study_id, entity_type, entity_id)
            
            # 更新缓存
            if self.enable_caching:
                self._update_cache(index_key, index_entry)
            
            logger.info(f"为 {entity_type}:{entity_id} 创建索引成功")
            return True
            
        except Exception as e:
            logger.error(f"创建索引失败: {e}")
            return False
    
    def update_index(self, entity_id: str, entity_type: str, 
                    updated_metadata: Dict) -> bool:
        """
        更新实体索引
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            updated_metadata: 更新的元数据
            
        Returns:
            更新是否成功
        """
        index_key = f"{entity_type}:{entity_id}"
        
        # 检查索引是否存在
        index_entry = self.get_index_entry(entity_id, entity_type)
        if not index_entry:
            logger.error(f"索引不存在: {index_key}")
            return False
        
        try:
            # 加载主索引
            main_index = self._load_index(self.main_index_file)
            
            # 更新元数据
            index_entry['metadata'].update(updated_metadata)
            index_entry['last_updated'] = datetime.now().isoformat()
            main_index[index_key] = index_entry
            
            # 保存主索引
            self._save_index(self.main_index_file, main_index)
            
            # 更新缓存
            if self.enable_caching and index_key in self.cache:
                self._update_cache(index_key, index_entry)
            
            # 处理元数据中患者ID或研究ID的变化
            old_patient_id = index_entry.get('metadata', {}).get('patient_id')
            new_patient_id = updated_metadata.get('patient_id')
            if old_patient_id != new_patient_id:
                self._update_patient_reference(entity_id, entity_type, old_patient_id, new_patient_id)
            
            old_study_id = index_entry.get('metadata', {}).get('study_id')
            new_study_id = updated_metadata.get('study_id')
            if old_study_id != new_study_id:
                self._update_study_reference(entity_id, entity_type, old_study_id, new_study_id)
            
            logger.info(f"更新索引 {index_key} 成功")
            return True
            
        except Exception as e:
            logger.error(f"更新索引失败: {e}")
            return False
    
    def delete_index(self, entity_id: str, entity_type: str) -> bool:
        """
        删除实体索引
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            
        Returns:
            删除是否成功
        """
        index_key = f"{entity_type}:{entity_id}"
        
        # 检查索引是否存在
        index_entry = self.get_index_entry(entity_id, entity_type)
        if not index_entry:
            logger.error(f"索引不存在: {index_key}")
            return False
        
        try:
            # 加载主索引
            main_index = self._load_index(self.main_index_file)
            
            # 获取需要更新的关联信息
            patient_id = index_entry.get('metadata', {}).get('patient_id')
            study_id = index_entry.get('metadata', {}).get('study_id')
            
            # 从主索引中删除
            if index_key in main_index:
                del main_index[index_key]
                self._save_index(self.main_index_file, main_index)
            
            # 更新类型索引
            self._update_type_index(entity_type, entity_id, remove=True)
            
            # 更新患者索引
            if patient_id:
                self._update_patient_index(patient_id, entity_type, entity_id, remove=True)
            
            # 更新研究索引
            if study_id:
                self._update_study_index(study_id, entity_type, entity_id, remove=True)
            
            # 从缓存中删除
            if self.enable_caching and index_key in self.cache:
                del self.cache[index_key]
                if index_key in self.cache_lru:
                    self.cache_lru.remove(index_key)
            
            logger.info(f"删除索引 {index_key} 成功")
            return True
            
        except Exception as e:
            logger.error(f"删除索引失败: {e}")
            return False
    
    def get_index_entry(self, entity_id: str, entity_type: str) -> Optional[Dict]:
        """
        获取索引条目
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            
        Returns:
            索引条目，如果不存在返回None
        """
        index_key = f"{entity_type}:{entity_id}"
        
        # 先检查缓存
        if self.enable_caching and index_key in self.cache:
            # 更新缓存LRU
            if index_key in self.cache_lru:
                self.cache_lru.remove(index_key)
            self.cache_lru.insert(0, index_key)
            return self.cache[index_key]
        
        # 从主索引中获取
        main_index = self._load_index(self.main_index_file)
        entry = main_index.get(index_key)
        
        # 更新缓存
        if entry and self.enable_caching:
            self._update_cache(index_key, entry)
        
        return entry
    
    def search_by_metadata(self, query: Dict, entity_types: Optional[List[str]] = None,
                          limit: int = 100) -> List[Dict]:
        """
        根据元数据搜索索引
        
        Args:
            query: 查询条件，支持简单的键值对匹配
            entity_types: 实体类型过滤列表
            limit: 结果数量限制
            
        Returns:
            匹配的索引条目列表
        """
        results = []
        main_index = self._load_index(self.main_index_file)
        
        for index_key, entry in main_index.items():
            # 检查实体类型
            if entity_types and entry['type'] not in entity_types:
                continue
            
            # 检查查询条件
            match = True
            for key, value in query.items():
                if key not in entry['metadata'] or entry['metadata'][key] != value:
                    match = False
                    break
            
            if match:
                results.append(entry)
                
                # 检查是否达到限制
                if len(results) >= limit:
                    break
        
        return results
    
    def search_by_patient(self, patient_id: str, entity_types: Optional[List[str]] = None,
                         limit: int = 100) -> List[Dict]:
        """
        根据患者ID搜索索引
        
        Args:
            patient_id: 患者唯一标识符
            entity_types: 实体类型过滤列表
            limit: 结果数量限制
            
        Returns:
            匹配的索引条目列表
        """
        results = []
        
        # 加载患者索引
        patient_index = self._load_index(self.patient_index_file)
        
        # 获取该患者的所有实体
        patient_entities = patient_index.get(patient_id, {})
        
        for entity_type, entity_ids in patient_entities.items():
            # 检查实体类型
            if entity_types and entity_type not in entity_types:
                continue
            
            for entity_id in entity_ids:
                # 获取完整的索引条目
                entry = self.get_index_entry(entity_id, entity_type)
                if entry:
                    results.append(entry)
                    
                    # 检查是否达到限制
                    if len(results) >= limit:
                        return results
        
        return results
    
    def search_by_study(self, study_id: str, entity_types: Optional[List[str]] = None,
                       limit: int = 100) -> List[Dict]:
        """
        根据研究ID搜索索引
        
        Args:
            study_id: 研究唯一标识符
            entity_types: 实体类型过滤列表
            limit: 结果数量限制
            
        Returns:
            匹配的索引条目列表
        """
        results = []
        
        # 加载研究索引
        study_index = self._load_index(self.study_index_file)
        
        # 获取该研究的所有实体
        study_entities = study_index.get(study_id, {})
        
        for entity_type, entity_ids in study_entities.items():
            # 检查实体类型
            if entity_types and entity_type not in entity_types:
                continue
            
            for entity_id in entity_ids:
                # 获取完整的索引条目
                entry = self.get_index_entry(entity_id, entity_type)
                if entry:
                    results.append(entry)
                    
                    # 检查是否达到限制
                    if len(results) >= limit:
                        return results
        
        return results
    
    def get_related_entities(self, entity_id: str, entity_type: str,
                           relation_type: str = 'patient') -> List[Dict]:
        """
        获取相关联的实体
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            relation_type: 关联类型（'patient', 'study'）
            
        Returns:
            相关联的实体列表
        """
        # 获取实体的索引条目
        entry = self.get_index_entry(entity_id, entity_type)
        if not entry:
            return []
        
        # 根据关联类型获取相关实体
        if relation_type == 'patient':
            patient_id = entry.get('metadata', {}).get('patient_id')
            if patient_id:
                return self.search_by_patient(patient_id)
        elif relation_type == 'study':
            study_id = entry.get('metadata', {}).get('study_id')
            if study_id:
                return self.search_by_study(study_id)
        
        return []
    
    def _initialize_index_files(self):
        """
        初始化索引文件
        """
        # 初始化主索引
        if not os.path.exists(self.main_index_file):
            self._save_index(self.main_index_file, {})
        
        # 初始化类型索引
        if not os.path.exists(self.type_index_file):
            self._save_index(self.type_index_file, {})
        
        # 初始化患者索引
        if not os.path.exists(self.patient_index_file):
            self._save_index(self.patient_index_file, {})
        
        # 初始化研究索引
        if not os.path.exists(self.study_index_file):
            self._save_index(self.study_index_file, {})
    
    def _load_index(self, index_file: str) -> Dict:
        """
        加载索引文件
        
        Args:
            index_file: 索引文件路径
            
        Returns:
            索引数据字典
        """
        if not os.path.exists(index_file):
            return {}
        
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载索引文件失败 {index_file}: {e}")
            return {}
    
    def _save_index(self, index_file: str, index_data: Dict):
        """
        保存索引文件
        
        Args:
            index_file: 索引文件路径
            index_data: 索引数据字典
        """
        try:
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存索引文件失败 {index_file}: {e}")
            raise
    
    def _update_type_index(self, entity_type: str, entity_id: str, remove: bool = False):
        """
        更新类型索引
        
        Args:
            entity_type: 实体类型
            entity_id: 实体唯一标识符
            remove: 是否移除（而非添加）
        """
        type_index = self._load_index(self.type_index_file)
        
        if entity_type not in type_index:
            if remove:
                return  # 类型不存在，无需移除
            type_index[entity_type] = []
        
        if remove:
            if entity_id in type_index[entity_type]:
                type_index[entity_type].remove(entity_id)
                # 如果类型下没有实体了，移除该类型
                if not type_index[entity_type]:
                    del type_index[entity_type]
        else:
            if entity_id not in type_index[entity_type]:
                type_index[entity_type].append(entity_id)
        
        self._save_index(self.type_index_file, type_index)
    
    def _update_patient_index(self, patient_id: str, entity_type: str, 
                             entity_id: str, remove: bool = False):
        """
        更新患者索引
        
        Args:
            patient_id: 患者唯一标识符
            entity_type: 实体类型
            entity_id: 实体唯一标识符
            remove: 是否移除（而非添加）
        """
        patient_index = self._load_index(self.patient_index_file)
        
        if patient_id not in patient_index:
            if remove:
                return  # 患者不存在，无需移除
            patient_index[patient_id] = {}
        
        if entity_type not in patient_index[patient_id]:
            if remove:
                return  # 类型不存在，无需移除
            patient_index[patient_id][entity_type] = []
        
        if remove:
            if entity_id in patient_index[patient_id][entity_type]:
                patient_index[patient_id][entity_type].remove(entity_id)
                # 如果类型下没有实体了，移除该类型
                if not patient_index[patient_id][entity_type]:
                    del patient_index[patient_id][entity_type]
                # 如果患者下没有实体了，移除该患者
                if not patient_index[patient_id]:
                    del patient_index[patient_id]
        else:
            if entity_id not in patient_index[patient_id][entity_type]:
                patient_index[patient_id][entity_type].append(entity_id)
        
        self._save_index(self.patient_index_file, patient_index)
    
    def _update_study_index(self, study_id: str, entity_type: str, 
                           entity_id: str, remove: bool = False):
        """
        更新研究索引
        
        Args:
            study_id: 研究唯一标识符
            entity_type: 实体类型
            entity_id: 实体唯一标识符
            remove: 是否移除（而非添加）
        """
        study_index = self._load_index(self.study_index_file)
        
        if study_id not in study_index:
            if remove:
                return  # 研究不存在，无需移除
            study_index[study_id] = {}
        
        if entity_type not in study_index[study_id]:
            if remove:
                return  # 类型不存在，无需移除
            study_index[study_id][entity_type] = []
        
        if remove:
            if entity_id in study_index[study_id][entity_type]:
                study_index[study_id][entity_type].remove(entity_id)
                # 如果类型下没有实体了，移除该类型
                if not study_index[study_id][entity_type]:
                    del study_index[study_id][entity_type]
                # 如果研究下没有实体了，移除该研究
                if not study_index[study_id]:
                    del study_index[study_id]
        else:
            if entity_id not in study_index[study_id][entity_type]:
                study_index[study_id][entity_type].append(entity_id)
        
        self._save_index(self.study_index_file, study_index)
    
    def _update_patient_reference(self, entity_id: str, entity_type: str,
                                old_patient_id: Optional[str], 
                                new_patient_id: Optional[str]):
        """
        更新实体的患者引用
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            old_patient_id: 旧患者ID
            new_patient_id: 新患者ID
        """
        # 从旧患者索引中移除
        if old_patient_id:
            self._update_patient_index(old_patient_id, entity_type, entity_id, remove=True)
        
        # 添加到新患者索引中
        if new_patient_id:
            self._update_patient_index(new_patient_id, entity_type, entity_id)
    
    def _update_study_reference(self, entity_id: str, entity_type: str,
                               old_study_id: Optional[str], 
                               new_study_id: Optional[str]):
        """
        更新实体的研究引用
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            old_study_id: 旧研究ID
            new_study_id: 新研究ID
        """
        # 从旧研究索引中移除
        if old_study_id:
            self._update_study_index(old_study_id, entity_type, entity_id, remove=True)
        
        # 添加到新研究索引中
        if new_study_id:
            self._update_study_index(new_study_id, entity_type, entity_id)
    
    def _update_cache(self, key: str, value: Dict):
        """
        更新缓存
        
        Args:
            key: 缓存键
            value: 缓存值
        """
        # 从LRU列表中移除（如果存在）
        if key in self.cache_lru:
            self.cache_lru.remove(key)
        
        # 添加到缓存和LRU列表头部
        self.cache[key] = value
        self.cache_lru.insert(0, key)
        
        # 如果缓存大小超过限制，删除最久未使用的项
        if len(self.cache) > self.cache_size:
            lru_key = self.cache_lru.pop()
            del self.cache[lru_key]
    
    def get_index_statistics(self) -> Dict:
        """
        获取索引统计信息
        
        Returns:
            统计信息字典
        """
        # 加载各索引文件
        main_index = self._load_index(self.main_index_file)
        type_index = self._load_index(self.type_index_file)
        patient_index = self._load_index(self.patient_index_file)
        study_index = self._load_index(self.study_index_file)
        
        # 计算类型统计
        type_stats = {}
        for entity_type, entity_ids in type_index.items():
            type_stats[entity_type] = len(entity_ids)
        
        # 计算患者统计
        patient_count = len(patient_index)
        patient_entity_stats = {}
        for patient_id, entities in patient_index.items():
            for entity_type, entity_ids in entities.items():
                patient_entity_stats[entity_type] = \
                    patient_entity_stats.get(entity_type, 0) + len(entity_ids)
        
        # 计算研究统计
        study_count = len(study_index)
        
        # 构建统计结果
        stats = {
            'total_entities': len(main_index),
            'entity_types': type_stats,
            'patient_count': patient_count,
            'study_count': study_count,
            'patient_entity_distribution': patient_entity_stats,
            'index_files': {
                'main_index_entries': len(main_index),
                'type_index_size': len(type_index),
                'patient_index_size': patient_count,
                'study_index_size': study_count
            },
            'cache_status': {
                'enabled': self.enable_caching,
                'current_size': len(self.cache),
                'max_size': self.cache_size
            }
        }
        
        return stats
    
    def rebuild_index(self):
        """
        重建所有索引
        
        Returns:
            重建是否成功
        """
        try:
            # 备份当前索引
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_files = []
            
            for index_file in [self.main_index_file, self.type_index_file,
                             self.patient_index_file, self.study_index_file]:
                if os.path.exists(index_file):
                    backup_path = f"{index_file}.bak_{timestamp}"
                    os.rename(index_file, backup_path)
                    backup_files.append(backup_path)
            
            # 重新初始化索引
            self._initialize_index_files()
            
            logger.info(f"索引重建成功，备份文件: {backup_files}")
            return True
            
        except Exception as e:
            logger.error(f"重建索引失败: {e}")
            return False