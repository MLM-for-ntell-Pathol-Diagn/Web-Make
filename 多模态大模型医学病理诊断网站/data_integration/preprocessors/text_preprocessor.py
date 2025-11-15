"""文本数据预处理模块"""
import re
import json
import logging
from typing import Dict, Optional, List, Union
import jieba
import jieba.analyse
import zhconv

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextDataProcessor:
    """
    文本数据处理器
    提供病历、检验报告、影像报告等文本数据的清洗与标准化功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化文本数据处理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        # 加载停用词
        self.stopwords = self._load_stopwords(self.config.get('stopwords_path'))
        # 设置默认参数
        self.default_params = {
            "normalize_chinese": True,
            "remove_special_chars": True,
            "lowercase": True,
            "remove_extra_spaces": True
        }
        self.default_params.update(self.config)
        
        # 医学术语标准化映射（示例）
        self.medical_terms_map = {
            "高血压": "高血压病",
            "心梗": "心肌梗死",
            "心衰": "心力衰竭",
            "脑梗": "脑梗死",
            "脑出血": "脑出血性卒中",
            "MRI": "磁共振成像",
            "CT": "计算机断层扫描",
            "X光": "X线检查",
            "血常规": "血液常规检查"
        }
    
    def _load_stopwords(self, stopwords_path: Optional[str] = None) -> List[str]:
        """
        加载停用词表
        
        Args:
            stopwords_path: 停用词表路径
            
        Returns:
            停用词列表
        """
        # 默认停用词
        default_stopwords = [
            "的", "了", "和", "是", "在", "有", "我", "他", "她", "它", "这", "那", 
            "为", "以", "于", "由", "到", "对", "对于", "关于", "但", "而", "及", 
            "与", "或", "如果", "因为", "所以", "虽然", "但是", "不仅", "而且", "通过"
        ]
        
        if stopwords_path:
            try:
                with open(stopwords_path, 'r', encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
            except Exception as e:
                logger.error(f"加载停用词表失败: {e}")
                return default_stopwords
        
        return default_stopwords
    
    def process_medical_text(self, text: str, text_type: str = "medical_record", 
                            operations: Optional[Dict] = None) -> Dict:
        """
        处理医学文本
        
        Args:
            text: 输入文本
            text_type: 文本类型 ('medical_record', 'lab_report', 'imaging_report')
            operations: 处理操作参数
            
        Returns:
            处理结果
        """
        if not text:
            return {"original": "", "processed": "", "entities": [], "keywords": []}
        
        # 设置默认操作
        if operations is None:
            operations = self.default_params.copy()
        else:
            # 合并默认操作和传入的操作
            op_copy = self.default_params.copy()
            op_copy.update(operations)
            operations = op_copy
        
        # 处理文本
        processed_text = text
        
        # 去除多余空格
        if operations.get("remove_extra_spaces", True):
            processed_text = self._remove_extra_spaces(processed_text)
        
        # 中文规范化（繁转简）
        if operations.get("normalize_chinese", True):
            processed_text = zhconv.convert(processed_text, 'zh-hans')
        
        # 转小写
        if operations.get("lowercase", True):
            processed_text = processed_text.lower()
        
        # 去除特殊字符
        if operations.get("remove_special_chars", True):
            # 保留中文、英文、数字和部分医学符号
            processed_text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9.+-/():,%]', ' ', processed_text)
        
        # 医学术语标准化
        if operations.get("standardize_terms", True):
            processed_text = self._standardize_medical_terms(processed_text)
        
        # 提取实体（模拟实现）
        entities = []
        if operations.get("extract_entities", True):
            entities = self._extract_medical_entities(processed_text, text_type)
        
        # 提取关键词
        keywords = []
        if operations.get("extract_keywords", True):
            keywords = self._extract_keywords(processed_text)
        
        # 分词
        tokens = []
        if operations.get("tokenize", True):
            tokens = self._tokenize_text(processed_text)
        
        # 去除停用词
        filtered_tokens = []
        if tokens and operations.get("remove_stopwords", True):
            filtered_tokens = [token for token in tokens if token not in self.stopwords]
        
        return {
            "original": text,
            "processed": processed_text,
            "entities": entities,
            "keywords": keywords,
            "tokens": tokens,
            "filtered_tokens": filtered_tokens,
            "text_type": text_type
        }
    
    def _remove_extra_spaces(self, text: str) -> str:
        """
        去除多余空格
        
        Args:
            text: 输入文本
            
        Returns:
            处理后的文本
        """
        # 替换多个空格为单个空格
        text = re.sub(r'\s+', ' ', text)
        # 去除首尾空格
        return text.strip()
    
    def _standardize_medical_terms(self, text: str) -> str:
        """
        医学术语标准化
        
        Args:
            text: 输入文本
            
        Returns:
            标准化后的文本
        """
        # 使用替换映射进行标准化
        for term, standardized_term in self.medical_terms_map.items():
            # 使用正则表达式进行精确匹配替换
            text = re.sub(rf'\b{term}\b', standardized_term, text)
        
        return text
    
    def _extract_medical_entities(self, text: str, text_type: str) -> List[Dict]:
        """
        提取医学实体
        
        Args:
            text: 输入文本
            text_type: 文本类型
            
        Returns:
            实体列表
        """
        # 这是一个简化实现
        # 实际项目中可以使用NLP模型如BERT、CRF等进行实体识别
        entities = []
        
        # 根据文本类型使用不同的实体提取策略
        if text_type == "medical_record":
            # 提取疾病名称（模拟）
            diseases = re.findall(r'(高血压|糖尿病|冠心病|肺炎|肿瘤|癌症)', text)
            for disease in diseases:
                entities.append({
                    "type": "disease",
                    "value": disease,
                    "source": "regex"
                })
            
            # 提取症状（模拟）
            symptoms = re.findall(r'(头痛|发热|咳嗽|恶心|呕吐|胸闷|乏力)', text)
            for symptom in symptoms:
                entities.append({
                    "type": "symptom",
                    "value": symptom,
                    "source": "regex"
                })
        
        elif text_type == "lab_report":
            # 提取检验指标（模拟）
            indicators = re.findall(r'([a-zA-Z0-9_]+)\s*[:：]\s*([\d.]+)', text)
            for indicator, value in indicators:
                entities.append({
                    "type": "lab_indicator",
                    "name": indicator,
                    "value": value,
                    "source": "regex"
                })
        
        elif text_type == "imaging_report":
            # 提取影像发现（模拟）
            findings = re.findall(r'(结节|肿块|阴影|积液|增厚|扩大|缩小)', text)
            for finding in findings:
                entities.append({
                    "type": "finding",
                    "value": finding,
                    "source": "regex"
                })
        
        return entities
    
    def _extract_keywords(self, text: str, top_k: int = 10) -> List[str]:
        """
        提取关键词
        
        Args:
            text: 输入文本
            top_k: 提取的关键词数量
            
        Returns:
            关键词列表
        """
        try:
            # 使用TF-IDF提取关键词
            keywords = jieba.analyse.extract_tags(text, topK=top_k)
            return keywords
        except Exception as e:
            logger.error(f"关键词提取失败: {e}")
            # 备用方案：使用简单的词频统计
            words = jieba.lcut(text)
            # 过滤停用词和单个字符
            filtered_words = [word for word in words if word not in self.stopwords and len(word) > 1]
            # 统计词频
            from collections import Counter
            word_counts = Counter(filtered_words)
            # 返回频率最高的top_k个词
            return [word for word, _ in word_counts.most_common(top_k)]
    
    def _tokenize_text(self, text: str) -> List[str]:
        """
        分词
        
        Args:
            text: 输入文本
            
        Returns:
            分词结果
        """
        try:
            return jieba.lcut(text)
        except Exception as e:
            logger.error(f"分词失败: {e}")
            # 备用方案：简单按字符分割
            return list(text)
    
    def batch_process(self, texts: List[Dict], operations: Optional[Dict] = None) -> List[Dict]:
        """
        批量处理文本
        
        Args:
            texts: 文本数据列表，每项包含text和text_type
            operations: 处理操作参数
            
        Returns:
            处理结果列表
        """
        results = []
        
        for text_data in texts:
            try:
                text = text_data.get('text', '')
                text_type = text_data.get('text_type', 'medical_record')
                
                result = self.process_medical_text(text, text_type, operations)
                results.append(result)
                
            except Exception as e:
                logger.error(f"处理文本失败: {e}")
                # 添加错误标记的结果
                results.append({
                    "original": text_data.get('text', ''),
                    "processed": None,
                    "error": str(e)
                })
        
        return results
    
    def validate_text_format(self, text: str, expected_format: str = "medical_record") -> Dict:
        """
        验证文本格式
        
        Args:
            text: 输入文本
            expected_format: 期望的文本格式
            
        Returns:
            验证结果
        """
        # 实现简单的格式验证逻辑
        is_valid = True
        errors = []
        warnings = []
        
        # 检查文本长度
        if len(text) < 10:
            warnings.append("文本长度过短")
        
        # 根据不同类型进行特定验证
        if expected_format == "medical_record":
            # 检查是否包含必要字段（模拟）
            required_fields = ["主诉", "现病史", "既往史", "体格检查"]
            for field in required_fields:
                if field not in text:
                    warnings.append(f"可能缺少字段: {field}")
        
        elif expected_format == "lab_report":
            # 检查是否包含数值结果（模拟）
            if not re.search(r'[\d.]+', text):
                errors.append("未发现检验数值结果")
                is_valid = False
        
        elif expected_format == "imaging_report":
            # 检查是否包含影像描述和结论（模拟）
            if "影像所见" not in text and "检查所见" not in text:
                warnings.append("可能缺少影像描述部分")
            if "诊断意见" not in text and "结论" not in text:
                warnings.append("可能缺少诊断结论部分")
        
        return {
            "is_valid": is_valid,
            "errors": errors,
            "warnings": warnings,
            "text_length": len(text)
        }