"""
医学文档存储管理模块

该模块负责病历、检验报告、影像报告等文本类医学文档的存储、检索和管理。
"""
import os
import json
import uuid
import logging
import hashlib
from datetime import datetime
from typing import Dict, Optional, List, Any
import re

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DocumentStorageManager:
    """
    医学文档存储管理器
    提供文本类医学文档的存储、检索、更新和删除功能
    """
    
    def __init__(self, config: Dict):
        """
        初始化文档存储管理器
        
        Args:
            config: 配置参数，包含以下关键字段：
                - storage_path: 文档存储根路径
                - index_path: 索引存储路径
                - supported_formats: 支持的文档格式列表
                - chunk_size: 文档分块大小（字符数）
        """
        self.config = config
        self.storage_path = config.get('storage_path', './data/documents')
        self.index_path = config.get('index_path', './data/indexes')
        self.supported_formats = config.get('supported_formats', ['txt', 'json', 'xml', 'pdf', 'docx'])
        self.chunk_size = config.get('chunk_size', 1000)
        
        # 创建必要的目录
        os.makedirs(self.storage_path, exist_ok=True)
        os.makedirs(self.index_path, exist_ok=True)
        
        logger.info(f"文档存储管理器初始化完成，存储路径: {self.storage_path}")
    
    def store_document(self, content: str, metadata: Dict, document_type: str = 'report', 
                      format_type: str = 'txt') -> str:
        """
        存储文档
        
        Args:
            content: 文档内容
            metadata: 文档元数据
            document_type: 文档类型（如 'report', 'medical_record', 'lab_result' 等）
            format_type: 文档格式类型
            
        Returns:
            文档的唯一标识符（UUID）
        """
        # 生成唯一标识符
        document_id = str(uuid.uuid4())
        
        # 验证格式
        if format_type not in self.supported_formats:
            raise ValueError(f"不支持的文档格式: {format_type}")
        
        # 计算内容哈希值以检查重复
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        
        # 创建存储子目录（基于文档类型）
        type_dir = os.path.join(self.storage_path, document_type)
        os.makedirs(type_dir, exist_ok=True)
        
        # 保存文档文件
        file_name = f"{document_id}.{format_type}"
        file_path = os.path.join(type_dir, file_name)
        
        try:
            # 根据格式类型保存内容
            if format_type == 'txt':
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
            elif format_type == 'json':
                # 假设内容已经是JSON字符串
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
            else:
                # 对于其他格式，目前仍然以文本方式保存
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.warning(f"格式 {format_type} 未实现专用保存逻辑，以文本方式保存")
            
            # 准备元数据
            full_metadata = {
                'id': document_id,
                'document_type': document_type,
                'format': format_type,
                'file_name': file_name,
                'file_path': file_path,
                'content_size': len(content),
                'content_hash': content_hash,
                'storage_date': datetime.now().isoformat(),
                'metadata': metadata
            }
            
            # 保存元数据
            self._save_metadata(document_id, full_metadata)
            
            # 为文档创建索引（用于全文搜索）
            self._create_document_index(document_id, content, metadata)
            
            logger.info(f"文档 {document_id} 存储成功")
            return document_id
            
        except Exception as e:
            logger.error(f"存储文档失败: {e}")
            # 如果保存失败，尝试清理已创建的文件
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise
    
    def retrieve_document(self, document_id: str) -> Dict:
        """
        检索文档
        
        Args:
            document_id: 文档唯一标识符
            
        Returns:
            包含文档内容和元数据的字典
        """
        # 验证ID格式
        try:
            uuid.UUID(document_id)
        except ValueError:
            raise ValueError("无效的文档ID格式")
        
        # 获取元数据
        metadata = self.get_metadata(document_id)
        if not metadata:
            raise FileNotFoundError(f"未找到ID为 {document_id} 的文档")
        
        # 读取文档文件
        file_path = metadata.get('file_path')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文档文件不存在: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            result = {
                'id': document_id,
                'content': content,
                'metadata': metadata
            }
            
            return result
            
        except Exception as e:
            logger.error(f"检索文档失败: {e}")
            raise
    
    def get_metadata(self, document_id: str) -> Optional[Dict]:
        """
        获取文档元数据
        
        Args:
            document_id: 文档唯一标识符
            
        Returns:
            元数据字典，如果不存在返回None
        """
        metadata_file = os.path.join(self.index_path, f"{document_id}_metadata.json")
        
        if not os.path.exists(metadata_file):
            return None
        
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            return metadata
        except Exception as e:
            logger.error(f"读取元数据失败: {e}")
            return None
    
    def update_document(self, document_id: str, new_content: str, new_metadata: Optional[Dict] = None) -> bool:
        """
        更新文档内容和元数据
        
        Args:
            document_id: 文档唯一标识符
            new_content: 新的文档内容
            new_metadata: 要更新的元数据（可选）
            
        Returns:
            更新是否成功
        """
        # 获取现有元数据
        existing_metadata = self.get_metadata(document_id)
        if not existing_metadata:
            logger.error(f"未找到ID为 {document_id} 的文档元数据")
            return False
        
        file_path = existing_metadata.get('file_path')
        if not os.path.exists(file_path):
            logger.error(f"文档文件不存在: {file_path}")
            return False
        
        try:
            # 更新文档内容
            format_type = existing_metadata.get('format', 'txt')
            if format_type == 'txt' or format_type == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
            else:
                logger.warning(f"格式 {format_type} 未实现专用更新逻辑，以文本方式更新")
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
            
            # 更新元数据
            existing_metadata['content_size'] = len(new_content)
            existing_metadata['content_hash'] = hashlib.sha256(new_content.encode('utf-8')).hexdigest()
            existing_metadata['last_modified'] = datetime.now().isoformat()
            
            if new_metadata:
                existing_metadata['metadata'].update(new_metadata)
            
            # 保存更新后的元数据
            self._save_metadata(document_id, existing_metadata)
            
            # 更新索引
            self._create_document_index(document_id, new_content, existing_metadata['metadata'])
            
            logger.info(f"文档 {document_id} 更新成功")
            return True
            
        except Exception as e:
            logger.error(f"更新文档失败: {e}")
            return False
    
    def delete_document(self, document_id: str) -> bool:
        """
        删除文档及其元数据和索引
        
        Args:
            document_id: 文档唯一标识符
            
        Returns:
            删除是否成功
        """
        # 获取元数据以找到文件路径
        metadata = self.get_metadata(document_id)
        if not metadata:
            logger.error(f"未找到ID为 {document_id} 的文档元数据")
            return False
        
        # 删除文档文件
        file_path = metadata.get('file_path')
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"删除文档文件失败: {e}")
                return False
        
        # 删除元数据文件
        metadata_file = os.path.join(self.index_path, f"{document_id}_metadata.json")
        if os.path.exists(metadata_file):
            try:
                os.remove(metadata_file)
            except Exception as e:
                logger.error(f"删除元数据文件失败: {e}")
                return False
        
        # 删除索引文件
        index_file = os.path.join(self.index_path, f"{document_id}_index.json")
        if os.path.exists(index_file):
            try:
                os.remove(index_file)
            except Exception as e:
                logger.error(f"删除索引文件失败: {e}")
                return False
        
        logger.info(f"文档 {document_id} 删除成功")
        return True
    
    def search_documents(self, query_text: str = "", metadata_filters: Optional[Dict] = None, 
                        document_type: Optional[str] = None, 
                        limit: int = 50) -> List[Dict]:
        """
        搜索文档
        
        Args:
            query_text: 搜索文本
            metadata_filters: 元数据过滤条件
            document_type: 文档类型过滤
            limit: 结果数量限制
            
        Returns:
            匹配的文档列表
        """
        results = []
        scores = {}
        
        # 遍历所有索引文件
        for file_name in os.listdir(self.index_path):
            if not file_name.endswith('_index.json'):
                continue
            
            document_id = file_name.replace('_index.json', '')
            index_path = os.path.join(self.index_path, file_name)
            
            try:
                with open(index_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                # 检查文档类型
                if document_type and index.get('document_type') != document_type:
                    continue
                
                # 检查元数据过滤条件
                if metadata_filters:
                    metadata_match = True
                    for key, value in metadata_filters.items():
                        if key not in index.get('metadata', {}) or index['metadata'][key] != value:
                            metadata_match = False
                            break
                    if not metadata_match:
                        continue
                
                # 文本搜索
                score = 0
                if query_text:
                    # 简单的关键词匹配计分
                    query_terms = re.findall(r'\b\w+\b', query_text.lower())
                    
                    # 检查标题
                    title = index.get('metadata', {}).get('title', '').lower()
                    for term in query_terms:
                        if term in title:
                            score += 2  # 标题匹配权重更高
                    
                    # 检查内容关键词
                    keywords = index.get('keywords', [])
                    for term in query_terms:
                        if term in keywords:
                            score += 1
                    
                    # 只有得分大于0的才添加到结果
                    if score == 0:
                        continue
                
                # 获取完整的元数据
                metadata = self.get_metadata(document_id)
                if metadata:
                    scores[document_id] = score
                    results.append(metadata)
                    
            except Exception as e:
                logger.warning(f"处理索引文件 {file_name} 时出错: {e}")
        
        # 按相关性得分排序
        if query_text:
            results.sort(key=lambda x: scores.get(x['id'], 0), reverse=True)
        
        # 限制结果数量
        return results[:limit]
    
    def list_documents(self, document_type: Optional[str] = None, 
                      start_date: Optional[datetime] = None, 
                      end_date: Optional[datetime] = None, 
                      limit: int = 100) -> List[Dict]:
        """
        列出文档
        
        Args:
            document_type: 文档类型过滤
            start_date: 开始日期
            end_date: 结束日期
            limit: 结果数量限制
            
        Returns:
            文档元数据列表
        """
        results = []
        
        # 遍历所有元数据文件
        for file_name in os.listdir(self.index_path):
            if not file_name.endswith('_metadata.json'):
                continue
            
            document_id = file_name.replace('_metadata.json', '')
            
            # 获取元数据
            metadata = self.get_metadata(document_id)
            if not metadata:
                continue
            
            # 检查文档类型
            if document_type and metadata.get('document_type') != document_type:
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
        
        # 按存储日期排序
        results.sort(key=lambda x: x['storage_date'], reverse=True)
        
        return results
    
    def _save_metadata(self, document_id: str, metadata: Dict):
        """
        保存元数据到文件
        
        Args:
            document_id: 文档唯一标识符
            metadata: 元数据字典
        """
        metadata_file = os.path.join(self.index_path, f"{document_id}_metadata.json")
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    def _create_document_index(self, document_id: str, content: str, metadata: Dict):
        """
        为文档创建索引
        
        Args:
            document_id: 文档唯一标识符
            content: 文档内容
            metadata: 文档元数据
        """
        # 提取关键词（简单实现，实际可能需要更复杂的NLP处理）
        # 去除特殊字符，提取单词
        words = re.findall(r'\b\w{3,}\b', content.lower())
        
        # 过滤常见停用词
        stop_words = {'the', 'and', 'is', 'in', 'to', 'of', 'a', 'with', 'for', 'on', 'are', 
                     'as', 'by', 'this', 'from', 'that', 'have', 'it', 'at', 'be', 'or', 
                     'which', 'an', 'but', 'not', 'has', 'all', 'were', 'when', 'been'}
        keywords = [word for word in words if word not in stop_words]
        
        # 统计词频，只保留出现次数大于1的词
        word_freq = {}
        for word in keywords:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # 获取最常见的关键词（最多50个）
        sorted_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        top_keywords = [word for word, freq in sorted_keywords[:50]]
        
        # 构建索引
        index = {
            'id': document_id,
            'document_type': metadata.get('document_type', 'general'),
            'title': metadata.get('title', ''),
            'keywords': top_keywords,
            'metadata': metadata,
            'index_date': datetime.now().isoformat()
        }
        
        # 保存索引
        index_file = os.path.join(self.index_path, f"{document_id}_index.json")
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
    
    def chunk_document(self, content: str) -> List[Dict]:
        """
        将长文档分块
        
        Args:
            content: 完整文档内容
            
        Returns:
            文档块列表，每个块包含内容和位置信息
        """
        chunks = []
        start = 0
        length = len(content)
        
        while start < length:
            end = min(start + self.chunk_size, length)
            
            # 尝试在句子边界分割
            if end < length:
                # 寻找最近的句号、问号、感叹号或换行符
                for i in range(end, max(start, end - 50), -1):
                    if content[i] in ['.', '?', '!', '\n']:
                        end = i + 1
                        break
            
            chunks.append({
                'content': content[start:end],
                'start_pos': start,
                'end_pos': end,
                'chunk_id': f"chunk_{len(chunks)}"
            })
            
            start = end
        
        return chunks
    
    def get_storage_statistics(self) -> Dict:
        """
        获取存储统计信息
        
        Returns:
            统计信息字典
        """
        stats = {
            'total_documents': 0,
            'total_size_bytes': 0,
            'type_distribution': {},
            'format_distribution': {},
            'monthly_distribution': {}
        }
        
        # 统计元数据文件
        for file_name in os.listdir(self.index_path):
            if not file_name.endswith('_metadata.json'):
                continue
            
            stats['total_documents'] += 1
            
            # 读取元数据获取更多信息
            document_id = file_name.replace('_metadata.json', '')
            metadata = self.get_metadata(document_id)
            if metadata:
                # 累加文档大小
                stats['total_size_bytes'] += metadata.get('content_size', 0)
                
                # 更新类型分布
                doc_type = metadata.get('document_type', 'general')
                stats['type_distribution'][doc_type] = \
                    stats['type_distribution'].get(doc_type, 0) + 1
                
                # 更新格式分布
                doc_format = metadata.get('format', 'txt')
                stats['format_distribution'][doc_format] = \
                    stats['format_distribution'].get(doc_format, 0) + 1
                
                # 更新月度分布
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