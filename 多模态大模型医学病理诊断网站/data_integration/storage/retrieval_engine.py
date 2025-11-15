"""
多模态检索引擎模块

该模块提供跨模态医学数据的统一检索服务，支持图像、文本、时间序列等多种数据类型的关联查询和语义搜索。
"""
import os
import json
import logging
import re
from datetime import datetime
from typing import Dict, Optional, List, Any, Union, Tuple
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiModalRetrievalEngine:
    """
    多模态检索引擎
    提供跨模态数据的统一检索接口，支持各种查询方式和结果排序
    """
    
    def __init__(self, config: Dict):
        """
        初始化多模态检索引擎
        
        Args:
            config: 配置参数，包含以下关键字段：
                - metadata_index_manager: 元数据索引管理器实例
                - image_storage_manager: 图像存储管理器实例（可选）
                - document_storage_manager: 文档存储管理器实例（可选）
                - default_limit: 默认结果数量限制
                - enable_ranking: 是否启用结果排序
        """
        self.config = config
        self.metadata_index_manager = config.get('metadata_index_manager')
        self.image_storage_manager = config.get('image_storage_manager')
        self.document_storage_manager = config.get('document_storage_manager')
        self.default_limit = config.get('default_limit', 50)
        self.enable_ranking = config.get('enable_ranking', True)
        
        logger.info("多模态检索引擎初始化完成")
    
    def search(self, query: Union[str, Dict], 
               modalities: Optional[List[str]] = None,
               filters: Optional[Dict] = None,
               sort_by: Optional[str] = 'relevance',
               limit: Optional[int] = None) -> Dict:
        """
        统一搜索接口
        
        Args:
            query: 搜索查询（字符串或字典）
            modalities: 模态过滤列表（如 ['image', 'document', 'timeseries']）
            filters: 额外过滤条件
            sort_by: 排序方式 ('relevance', 'date', 'created_at', 'modified_at')
            limit: 结果数量限制
            
        Returns:
            搜索结果字典，包含各模态的结果和聚合信息
        """
        if limit is None:
            limit = self.default_limit
        
        results = {
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'total_results': 0,
            'modality_results': {},
            'aggregations': {}
        }
        
        # 根据查询类型执行相应的搜索
        if isinstance(query, str):
            # 文本搜索
            text_results = self._search_text(query, modalities, filters, limit)
            results['modality_results'].update(text_results)
        elif isinstance(query, dict):
            # 结构化查询
            structured_results = self._search_structured(query, modalities, filters, limit)
            results['modality_results'].update(structured_results)
        else:
            raise ValueError("查询必须是字符串或字典类型")
        
        # 计算总结果数
        results['total_results'] = sum(len(items) for items in results['modality_results'].values())
        
        # 生成聚合统计
        results['aggregations'] = self._generate_aggregations(results['modality_results'])
        
        # 如果启用排序，对结果进行排序
        if self.enable_ranking and sort_by:
            results['modality_results'] = self._sort_results(results['modality_results'], sort_by)
        
        return results
    
    def search_by_patient(self, patient_id: str, 
                         modalities: Optional[List[str]] = None,
                         filters: Optional[Dict] = None,
                         sort_by: Optional[str] = 'date',
                         limit: Optional[int] = None) -> Dict:
        """
        根据患者ID搜索
        
        Args:
            patient_id: 患者唯一标识符
            modalities: 模态过滤列表
            filters: 额外过滤条件
            sort_by: 排序方式
            limit: 结果数量限制
            
        Returns:
            搜索结果字典
        """
        if limit is None:
            limit = self.default_limit
        
        # 使用元数据索引管理器搜索
        if not self.metadata_index_manager:
            raise ValueError("元数据索引管理器未初始化")
        
        raw_results = self.metadata_index_manager.search_by_patient(
            patient_id, modalities, limit=limit * 2  # 多获取一些结果用于过滤
        )
        
        # 应用额外过滤条件
        filtered_results = self._apply_filters(raw_results, filters)
        
        # 按模态分组
        grouped_results = self._group_by_modality(filtered_results)
        
        # 限制每种模态的结果数量
        for modality in grouped_results:
            grouped_results[modality] = grouped_results[modality][:limit]
        
        # 排序
        if sort_by:
            grouped_results = self._sort_results(grouped_results, sort_by)
        
        results = {
            'query': {'patient_id': patient_id},
            'timestamp': datetime.now().isoformat(),
            'total_results': len(filtered_results),
            'modality_results': grouped_results,
            'aggregations': self._generate_aggregations(grouped_results)
        }
        
        return results
    
    def search_by_study(self, study_id: str, 
                       modalities: Optional[List[str]] = None,
                       filters: Optional[Dict] = None,
                       sort_by: Optional[str] = 'date',
                       limit: Optional[int] = None) -> Dict:
        """
        根据研究ID搜索
        
        Args:
            study_id: 研究唯一标识符
            modalities: 模态过滤列表
            filters: 额外过滤条件
            sort_by: 排序方式
            limit: 结果数量限制
            
        Returns:
            搜索结果字典
        """
        if limit is None:
            limit = self.default_limit
        
        # 使用元数据索引管理器搜索
        if not self.metadata_index_manager:
            raise ValueError("元数据索引管理器未初始化")
        
        raw_results = self.metadata_index_manager.search_by_study(
            study_id, modalities, limit=limit * 2  # 多获取一些结果用于过滤
        )
        
        # 应用额外过滤条件
        filtered_results = self._apply_filters(raw_results, filters)
        
        # 按模态分组
        grouped_results = self._group_by_modality(filtered_results)
        
        # 限制每种模态的结果数量
        for modality in grouped_results:
            grouped_results[modality] = grouped_results[modality][:limit]
        
        # 排序
        if sort_by:
            grouped_results = self._sort_results(grouped_results, sort_by)
        
        results = {
            'query': {'study_id': study_id},
            'timestamp': datetime.now().isoformat(),
            'total_results': len(filtered_results),
            'modality_results': grouped_results,
            'aggregations': self._generate_aggregations(grouped_results)
        }
        
        return results
    
    def get_related_entities(self, entity_id: str, 
                           entity_type: str,
                           relation_type: str = 'patient',
                           modalities: Optional[List[str]] = None,
                           limit: Optional[int] = None) -> Dict:
        """
        获取相关联的实体
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            relation_type: 关联类型（'patient', 'study'）
            modalities: 模态过滤列表
            limit: 结果数量限制
            
        Returns:
            相关实体结果字典
        """
        if limit is None:
            limit = self.default_limit
        
        # 使用元数据索引管理器获取相关实体
        if not self.metadata_index_manager:
            raise ValueError("元数据索引管理器未初始化")
        
        related_entities = self.metadata_index_manager.get_related_entities(
            entity_id, entity_type, relation_type
        )
        
        # 过滤模态
        if modalities:
            related_entities = [e for e in related_entities if e['type'] in modalities]
        
        # 排除自己
        related_entities = [e for e in related_entities 
                          if not (e['id'] == entity_id and e['type'] == entity_type)]
        
        # 按模态分组
        grouped_results = self._group_by_modality(related_entities)
        
        # 限制每种模态的结果数量
        for modality in grouped_results:
            grouped_results[modality] = grouped_results[modality][:limit]
        
        results = {
            'related_to': {'id': entity_id, 'type': entity_type},
            'relation_type': relation_type,
            'timestamp': datetime.now().isoformat(),
            'total_results': len(related_entities),
            'modality_results': grouped_results,
            'aggregations': self._generate_aggregations(grouped_results)
        }
        
        return results
    
    def _search_text(self, query_text: str, 
                    modalities: Optional[List[str]] = None,
                    filters: Optional[Dict] = None,
                    limit: int = 50) -> Dict[str, List[Dict]]:
        """
        执行文本搜索
        
        Args:
            query_text: 搜索文本
            modalities: 模态过滤列表
            filters: 额外过滤条件
            limit: 结果数量限制
            
        Returns:
            按模态分组的结果
        """
        results = defaultdict(list)
        
        # 1. 使用元数据索引搜索包含查询文本的元数据
        if self.metadata_index_manager:
            # 简单实现：搜索包含查询关键词的元数据字段
            # 注意：实际应用中可能需要更复杂的全文搜索实现
            metadata_matches = self._search_metadata_with_text(query_text, modalities)
            results.update(self._group_by_modality(metadata_matches))
        
        # 2. 使用文档存储管理器搜索文本内容
        if self.document_storage_manager and (not modalities or 'document' in modalities):
            doc_results = self.document_storage_manager.search_documents(
                query_text=query_text,
                metadata_filters=filters,
                limit=limit
            )
            
            # 转换为标准格式
            for doc in doc_results:
                results['document'].append({
                    'id': doc['id'],
                    'type': 'document',
                    'metadata': doc,
                    'score': 1.0  # 简化实现，实际应有真实的相关性分数
                })
        
        # 应用额外过滤条件
        filtered_results = {}
        for modality, items in results.items():
            filtered_items = self._apply_filters(items, filters)
            filtered_results[modality] = filtered_items[:limit]  # 限制数量
        
        return dict(filtered_results)
    
    def _search_structured(self, query: Dict, 
                          modalities: Optional[List[str]] = None,
                          filters: Optional[Dict] = None,
                          limit: int = 50) -> Dict[str, List[Dict]]:
        """
        执行结构化搜索
        
        Args:
            query: 结构化查询条件
            modalities: 模态过滤列表
            filters: 额外过滤条件
            limit: 结果数量限制
            
        Returns:
            按模态分组的结果
        """
        results = defaultdict(list)
        
        # 使用元数据索引管理器执行结构化查询
        if self.metadata_index_manager:
            # 合并查询条件和过滤条件
            combined_query = query.copy()
            if filters:
                combined_query.update(filters)
            
            metadata_results = self.metadata_index_manager.search_by_metadata(
                combined_query,
                entity_types=modalities,
                limit=limit * 3  # 多获取一些结果用于分组和进一步过滤
            )
            
            # 按模态分组并限制数量
            grouped_results = self._group_by_modality(metadata_results)
            for modality, items in grouped_results.items():
                results[modality] = items[:limit]
        
        return dict(results)
    
    def _search_metadata_with_text(self, query_text: str, 
                                  modalities: Optional[List[str]] = None) -> List[Dict]:
        """
        在元数据中搜索文本
        
        Args:
            query_text: 搜索文本
            modalities: 模态过滤列表
            
        Returns:
            匹配的结果列表
        """
        matches = []
        query_lower = query_text.lower()
        
        # 获取所有索引条目（简化实现，实际应该有更高效的方式）
        if self.metadata_index_manager:
            # 由于没有直接的方法获取所有条目，这里使用一个小技巧：
            # 1. 先获取所有类型
            # 2. 对每种类型，尝试使用一个可能存在的通用字段进行查询
            # 注意：这只是一个演示实现，实际应用中需要更高效的索引
            
            # 简化处理：获取所有患者ID，然后查询每个患者的数据
            # 这不是最高效的方法，但可以作为演示
            try:
                # 这里假设我们可以访问患者索引
                import os
                patient_index_file = os.path.join(self.metadata_index_manager.index_path, 'patient_index.json')
                if os.path.exists(patient_index_file):
                    with open(patient_index_file, 'r', encoding='utf-8') as f:
                        patient_index = json.load(f)
                    
                    for patient_id in patient_index.keys():
                        patient_results = self.metadata_index_manager.search_by_patient(
                            patient_id, modalities, limit=100
                        )
                        
                        for result in patient_results:
                            # 检查元数据中是否包含查询文本
                            if self._check_text_in_metadata(result['metadata'], query_lower):
                                matches.append(result)
            except Exception as e:
                logger.warning(f"元数据文本搜索失败: {e}")
        
        return matches
    
    def _check_text_in_metadata(self, metadata: Dict, query_lower: str) -> bool:
        """
        检查元数据中是否包含查询文本
        
        Args:
            metadata: 元数据字典
            query_lower: 小写的查询文本
            
        Returns:
            是否包含
        """
        # 检查常用文本字段
        text_fields = ['title', 'description', 'diagnosis', 'findings', 'notes', 'summary']
        
        for field in text_fields:
            if field in metadata:
                value = str(metadata[field]).lower()
                if query_lower in value:
                    return True
        
        # 检查所有字符串字段
        for key, value in metadata.items():
            if isinstance(value, str):
                if query_lower in value.lower():
                    return True
            elif isinstance(value, list):
                # 检查列表中的字符串
                for item in value:
                    if isinstance(item, str) and query_lower in item.lower():
                        return True
        
        return False
    
    def _apply_filters(self, results: List[Dict], 
                      filters: Optional[Dict]) -> List[Dict]:
        """
        应用过滤条件
        
        Args:
            results: 原始结果列表
            filters: 过滤条件字典
            
        Returns:
            过滤后的结果列表
        """
        if not filters:
            return results
        
        filtered = []
        
        for result in results:
            match = True
            metadata = result.get('metadata', {})
            
            for key, value in filters.items():
                # 支持简单的相等匹配
                if key not in metadata or metadata[key] != value:
                    match = False
                    break
                
                # 支持范围过滤（如果值是字典并包含min/max键）
                if isinstance(value, dict):
                    if 'min' in value and metadata[key] < value['min']:
                        match = False
                        break
                    if 'max' in value and metadata[key] > value['max']:
                        match = False
                        break
                    
                    # 支持in操作符
                    if 'in' in value and metadata[key] not in value['in']:
                        match = False
                        break
            
            if match:
                filtered.append(result)
        
        return filtered
    
    def _group_by_modality(self, results: List[Dict]) -> Dict[str, List[Dict]]:
        """
        按模态分组结果
        
        Args:
            results: 结果列表
            
        Returns:
            按模态分组的字典
        """
        grouped = defaultdict(list)
        
        for result in results:
            modality = result.get('type', 'unknown')
            grouped[modality].append(result)
        
        return dict(grouped)
    
    def _sort_results(self, grouped_results: Dict[str, List[Dict]], 
                     sort_by: str) -> Dict[str, List[Dict]]:
        """
        对结果进行排序
        
        Args:
            grouped_results: 按模态分组的结果
            sort_by: 排序方式
            
        Returns:
            排序后的结果
        """
        sorted_results = {}
        
        for modality, items in grouped_results.items():
            if sort_by == 'relevance':
                # 按相关性排序（假设结果中有score字段）
                sorted_items = sorted(items, 
                                    key=lambda x: x.get('score', 0), 
                                    reverse=True)
            elif sort_by in ['date', 'created_at', 'storage_date']:
                # 按日期排序
                date_field = 'storage_date'
                if sort_by == 'created_at':
                    date_field = 'created_at'
                elif sort_by == 'date' and 'metadata' in items[0] and 'date' in items[0]['metadata']:
                    date_field = 'date'
                
                sorted_items = sorted(items, 
                                    key=lambda x: self._get_date_value(x, date_field), 
                                    reverse=True)
            elif sort_by == 'modified_at' or sort_by == 'last_updated':
                # 按最后更新时间排序
                sorted_items = sorted(items, 
                                    key=lambda x: self._get_date_value(x, 'last_updated'), 
                                    reverse=True)
            else:
                # 默认为相关性排序
                sorted_items = sorted(items, 
                                    key=lambda x: x.get('score', 0), 
                                    reverse=True)
            
            sorted_results[modality] = sorted_items
        
        return sorted_results
    
    def _get_date_value(self, item: Dict, date_field: str) -> datetime:
        """
        从结果项中获取日期值
        
        Args:
            item: 结果项
            date_field: 日期字段名
            
        Returns:
            日期对象，如果无法解析则返回默认值
        """
        # 先尝试直接从item中获取
        if date_field in item:
            date_str = item[date_field]
        elif 'metadata' in item and date_field in item['metadata']:
            date_str = item['metadata'][date_field]
        else:
            # 返回一个很早的日期作为默认值
            return datetime(1970, 1, 1)
        
        try:
            if isinstance(date_str, str):
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            elif isinstance(date_str, datetime):
                return date_str
        except Exception as e:
            logger.warning(f"解析日期失败 {date_str}: {e}")
        
        return datetime(1970, 1, 1)
    
    def _generate_aggregations(self, grouped_results: Dict[str, List[Dict]]) -> Dict:
        """
        生成聚合统计信息
        
        Args:
            grouped_results: 按模态分组的结果
            
        Returns:
            聚合统计信息字典
        """
        aggregations = {
            'by_modality': {},
            'total': 0
        }
        
        # 模态统计
        for modality, items in grouped_results.items():
            count = len(items)
            aggregations['by_modality'][modality] = count
            aggregations['total'] += count
        
        # 提取常见的元数据值分布
        metadata_aggregations = defaultdict(lambda: defaultdict(int))
        fields_to_aggregate = ['document_type', 'image_type', 'diagnosis', 'body_part']
        
        for items in grouped_results.values():
            for item in items:
                metadata = item.get('metadata', {})
                for field in fields_to_aggregate:
                    if field in metadata:
                        value = str(metadata[field])
                        metadata_aggregations[field][value] += 1
        
        # 添加前N个常见值
        for field, counts in metadata_aggregations.items():
            # 只保留计数大于1的，按计数排序
            sorted_values = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5]
            if sorted_values:
                aggregations[f'top_{field}'] = dict(sorted_values)
        
        return aggregations
    
    def get_similar_entities(self, entity_id: str, 
                           entity_type: str,
                           modalities: Optional[List[str]] = None,
                           limit: int = 10) -> Dict:
        """
        获取相似实体（简化实现）
        
        Args:
            entity_id: 实体唯一标识符
            entity_type: 实体类型
            modalities: 模态过滤列表
            limit: 结果数量限制
            
        Returns:
            相似实体结果
        """
        # 获取实体的元数据
        if not self.metadata_index_manager:
            raise ValueError("元数据索引管理器未初始化")
        
        entity = self.metadata_index_manager.get_index_entry(entity_id, entity_type)
        if not entity:
            raise ValueError(f"未找到实体 {entity_type}:{entity_id}")
        
        # 提取关键元数据用于相似性匹配
        entity_metadata = entity.get('metadata', {})
        
        # 构建相似性查询
        similarity_query = {}
        # 尝试使用患者ID、研究ID等标识符字段
        for key in ['patient_id', 'study_id', 'case_id']:
            if key in entity_metadata:
                similarity_query[key] = entity_metadata[key]
                break
        
        # 如果没有找到标识符，使用其他关键字段
        if not similarity_query:
            for key in ['diagnosis', 'body_part', 'tissue_type']:
                if key in entity_metadata:
                    similarity_query[key] = entity_metadata[key]
                    break
        
        # 执行搜索
        if similarity_query:
            results = self.search(
                similarity_query,
                modalities=modalities,
                limit=limit * 2  # 获取更多结果用于排除自身
            )
            
            # 从结果中排除自身
            for modality in results['modality_results']:
                results['modality_results'][modality] = [
                    item for item in results['modality_results'][modality]
                    if not (item['id'] == entity_id and item['type'] == entity_type)
                ][:limit]  # 重新限制数量
            
            # 重新计算统计
            results['total_results'] = sum(len(items) for items in results['modality_results'].values())
            results['aggregations'] = self._generate_aggregations(results['modality_results'])
            
            return results
        else:
            # 无法构建相似性查询
            return {
                'similar_to': {'id': entity_id, 'type': entity_type},
                'timestamp': datetime.now().isoformat(),
                'total_results': 0,
                'modality_results': {},
                'aggregations': {'by_modality': {}, 'total': 0}
            }