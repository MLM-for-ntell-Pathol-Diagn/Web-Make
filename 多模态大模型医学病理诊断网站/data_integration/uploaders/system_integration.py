"""医疗系统集成接口模块"""
from typing import Dict, Optional, List, Any
import requests
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MedicalSystemIntegrator:
    """
    医疗系统集成器
    提供与HIS、EMR、LIS、PACS等系统对接的API/SDK接口
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化医疗系统集成器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.systems = {
            "his": self.config.get("his", {}),
            "emr": self.config.get("emr", {}),
            "lis": self.config.get("lis", {}),
            "pacs": self.config.get("pacs", {})
        }
    
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        default_config = {
            "his": {
                "base_url": "http://localhost:8001/api",
                "auth": {
                    "username": "api_user",
                    "password": "api_password"
                }
            },
            "emr": {
                "base_url": "http://localhost:8002/api",
                "auth": {
                    "token": "default_token"
                }
            },
            "lis": {
                "base_url": "http://localhost:8003/api",
                "auth": {
                    "username": "lis_user",
                    "password": "lis_password"
                }
            },
            "pacs": {
                "base_url": "http://localhost:8004/api",
                "auth": {
                    "api_key": "pacs_api_key"
                }
            }
        }
        
        if config_path and isinstance(config_path, str):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    custom_config = json.load(f)
                    # 合并配置
                    for system in default_config:
                        if system in custom_config:
                            default_config[system].update(custom_config[system])
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}")
        
        return default_config
    
    def get_patient_info_from_his(self, patient_id: str) -> Optional[Dict]:
        """
        从HIS系统获取患者信息
        
        Args:
            patient_id: 患者ID
            
        Returns:
            患者信息
        """
        try:
            url = f"{self.systems['his']['base_url']}/patients/{patient_id}"
            auth = self.systems['his']['auth']
            
            # 根据认证方式选择请求方法
            if 'username' in auth and 'password' in auth:
                response = requests.get(url, auth=(auth['username'], auth['password']))
            else:
                response = requests.get(url, headers={"Authorization": f"Bearer {auth.get('token', '')}"})
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"从HIS系统获取患者信息失败: {e}")
            return None
    
    def get_medical_records_from_emr(self, patient_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """
        从EMR系统获取病历记录
        
        Args:
            patient_id: 患者ID
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            病历记录列表
        """
        try:
            url = f"{self.systems['emr']['base_url']}/patients/{patient_id}/records"
            params = {}
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            headers = {"Authorization": f"Bearer {self.systems['emr']['auth'].get('token', '')}"}
            response = requests.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"从EMR系统获取病历记录失败: {e}")
            return []
    
    def get_lab_results_from_lis(self, patient_id: str, test_type: Optional[str] = None) -> List[Dict]:
        """
        从LIS系统获取检验结果
        
        Args:
            patient_id: 患者ID
            test_type: 检验类型
            
        Returns:
            检验结果列表
        """
        try:
            url = f"{self.systems['lis']['base_url']}/patients/{patient_id}/lab_results"
            params = {}
            if test_type:
                params['test_type'] = test_type
            
            auth = self.systems['lis']['auth']
            response = requests.get(url, auth=(auth['username'], auth['password']), params=params)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"从LIS系统获取检验结果失败: {e}")
            return []
    
    def get_imaging_studies_from_pacs(self, patient_id: str, modality: Optional[str] = None) -> List[Dict]:
        """
        从PACS系统获取影像检查
        
        Args:
            patient_id: 患者ID
            modality: 影像模态 (CT, MRI, X-Ray等)
            
        Returns:
            影像检查列表
        """
        try:
            url = f"{self.systems['pacs']['base_url']}/patients/{patient_id}/studies"
            params = {}
            if modality:
                params['modality'] = modality
            
            headers = {"X-API-Key": self.systems['pacs']['auth'].get('api_key', '')}
            response = requests.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"从PACS系统获取影像检查失败: {e}")
            return []
    
    def import_patient_data(self, patient_id: str, data_types: List[str] = None) -> Dict[str, Any]:
        """
        导入患者多源数据
        
        Args:
            patient_id: 患者ID
            data_types: 要导入的数据类型列表 ['his', 'emr', 'lis', 'pacs']
            
        Returns:
            整合后的患者数据
        """
        if data_types is None:
            data_types = ['his', 'emr', 'lis', 'pacs']
        
        patient_data = {
            "patient_id": patient_id,
            "import_timestamp": self._get_current_timestamp()
        }
        
        # 根据指定的数据类型导入相应数据
        if 'his' in data_types:
            patient_data['his_info'] = self.get_patient_info_from_his(patient_id)
        
        if 'emr' in data_types:
            patient_data['medical_records'] = self.get_medical_records_from_emr(patient_id)
        
        if 'lis' in data_types:
            patient_data['lab_results'] = self.get_lab_results_from_lis(patient_id)
        
        if 'pacs' in data_types:
            patient_data['imaging_studies'] = self.get_imaging_studies_from_pacs(patient_id)
        
        # 这里可以添加数据导入后的处理逻辑
        self._process_imported_data(patient_data)
        
        return patient_data
    
    def _process_imported_data(self, patient_data: Dict[str, Any]) -> None:
        """
        处理导入的数据
        
        Args:
            patient_data: 患者数据
        """
        # 实际项目中这里可以添加数据清洗、标准化等处理
        logger.info(f"处理患者 {patient_data.get('patient_id')} 的导入数据")
    
    def _get_current_timestamp(self) -> str:
        """
        获取当前时间戳
        
        Returns:
            ISO格式的时间戳
        """
        from datetime import datetime
        return datetime.now().isoformat()
    
    def test_connection(self, system: str) -> bool:
        """
        测试与指定医疗系统的连接
        
        Args:
            system: 系统名称 ('his', 'emr', 'lis', 'pacs')
            
        Returns:
            连接是否成功
        """
        if system not in self.systems:
            logger.error(f"未知的系统: {system}")
            return False
        
        try:
            if system == 'his':
                url = f"{self.systems['his']['base_url']}/ping"
                auth = self.systems['his']['auth']
                if 'username' in auth and 'password' in auth:
                    response = requests.get(url, auth=(auth['username'], auth['password']), timeout=5)
                else:
                    response = requests.get(url, headers={"Authorization": f"Bearer {auth.get('token', '')}"}, timeout=5)
            
            elif system == 'emr':
                url = f"{self.systems['emr']['base_url']}/ping"
                headers = {"Authorization": f"Bearer {self.systems['emr']['auth'].get('token', '')}"}
                response = requests.get(url, headers=headers, timeout=5)
            
            elif system == 'lis':
                url = f"{self.systems['lis']['base_url']}/ping"
                auth = self.systems['lis']['auth']
                response = requests.get(url, auth=(auth['username'], auth['password']), timeout=5)
            
            elif system == 'pacs':
                url = f"{self.systems['pacs']['base_url']}/ping"
                headers = {"X-API-Key": self.systems['pacs']['auth'].get('api_key', '')}
                response = requests.get(url, headers=headers, timeout=5)
            
            return response.status_code == 200
        except Exception as e:
            logger.error(f"测试与 {system} 系统的连接失败: {e}")
            return False