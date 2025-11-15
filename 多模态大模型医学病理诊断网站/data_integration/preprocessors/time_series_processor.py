"""时间序列数据预处理模块"""
import numpy as np
import pandas as pd
import logging
from typing import Dict, Optional, List, Union, Tuple
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimeSeriesProcessor:
    """
    时间序列数据处理器
    提供血压、心率等生理数据的处理功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化时间序列数据处理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        # 设置默认参数
        self.default_params = {
            "resample_freq": "1H",  # 默认重采样频率为1小时
            "fill_method": "linear",  # 默认填充方法为线性插值
            "outlier_threshold": 3.0,  # 默认异常值阈值为3个标准差
            "min_data_points": 10  # 最小数据点数量
        }
        self.default_params.update(self.config)
        
        # 数据类型配置（示例）
        self.data_type_config = {
            "blood_pressure": {
                "systolic_range": (60, 200),  # 收缩压正常范围
                "diastolic_range": (40, 130),  # 舒张压正常范围
                "units": "mmHg"
            },
            "heart_rate": {
                "range": (40, 180),  # 心率正常范围
                "units": "bpm"
            },
            "respiratory_rate": {
                "range": (10, 30),  # 呼吸频率正常范围
                "units": "breaths/min"
            },
            "body_temperature": {
                "range": (35.0, 42.0),  # 体温正常范围
                "units": "°C"
            },
            "blood_glucose": {
                "range": (3.9, 6.1),  # 血糖正常范围（空腹）
                "units": "mmol/L"
            }
        }
    
    def process_time_series(self, data: Union[List[Dict], pd.DataFrame], 
                           data_type: str = "heart_rate", 
                           operations: Optional[Dict] = None) -> Dict:
        """
        处理时间序列数据
        
        Args:
            data: 输入数据（字典列表或DataFrame）
            data_type: 数据类型
            operations: 处理操作参数
            
        Returns:
            处理结果
        """
        # 设置默认操作
        if operations is None:
            operations = {
                "clean_data": True,
                "remove_outliers": True,
                "fill_missing": True,
                "resample": False,
                "calculate_features": True
            }
        
        # 转换数据为DataFrame
        df = self._convert_to_dataframe(data)
        if df.empty:
            return {"error": "输入数据为空"}
        
        # 确保数据有时间索引
        if "timestamp" in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp').sort_index()
        elif not isinstance(df.index, pd.DatetimeIndex):
            logger.error("数据缺少时间戳列或索引不是日期时间类型")
            return {"error": "数据缺少有效的时间戳"}
        
        # 获取数据类型配置
        data_config = self.data_type_config.get(data_type, {})
        
        # 处理数据
        result = {
            "original_data": df.copy(),
            "data_type": data_type,
            "processing_timestamp": datetime.now().isoformat()
        }
        
        # 数据清洗
        if operations.get("clean_data", True):
            df = self._clean_time_series(df, data_type, data_config)
            result["cleaned_data"] = df
        
        # 移除异常值
        if operations.get("remove_outliers", True):
            threshold = operations.get("outlier_threshold", self.default_params["outlier_threshold"])
            df = self._remove_outliers(df, threshold)
            result["outliers_removed"] = True
        
        # 填充缺失值
        if operations.get("fill_missing", True):
            method = operations.get("fill_method", self.default_params["fill_method"])
            df = self._fill_missing_values(df, method)
            result["missing_filled"] = True
            result["fill_method"] = method
        
        # 重采样
        if operations.get("resample", True):
            freq = operations.get("resample_freq", self.default_params["resample_freq"])
            df = self._resample_data(df, freq)
            result["resampled"] = True
            result["resample_freq"] = freq
        
        # 计算特征
        if operations.get("calculate_features", True):
            features = self._calculate_time_series_features(df, data_type)
            result["features"] = features
        
        # 质量评估
        quality = self._assess_data_quality(df, data_config)
        result["data_quality"] = quality
        
        # 存储最终处理后的数据
        result["processed_data"] = df
        
        return result
    
    def _convert_to_dataframe(self, data: Union[List[Dict], pd.DataFrame]) -> pd.DataFrame:
        """
        将输入数据转换为DataFrame
        
        Args:
            data: 输入数据
            
        Returns:
            DataFrame对象
        """
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            logger.error("不支持的数据格式")
            return pd.DataFrame()
    
    def _clean_time_series(self, df: pd.DataFrame, data_type: str, 
                          data_config: Dict) -> pd.DataFrame:
        """
        清洗时间序列数据
        
        Args:
            df: 输入DataFrame
            data_type: 数据类型
            data_config: 数据类型配置
            
        Returns:
            清洗后的DataFrame
        """
        # 创建副本以避免修改原始数据
        cleaned_df = df.copy()
        
        # 根据数据类型进行特定的清洗
        if data_type == "blood_pressure":
            # 血压数据通常包含收缩压和舒张压
            if "systolic" in cleaned_df.columns and "diastolic" in cleaned_df.columns:
                # 应用正常范围过滤
                systolic_range = data_config.get("systolic_range", (60, 200))
                diastolic_range = data_config.get("diastolic_range", (40, 130))
                
                # 设置收缩压和舒张压的有效范围
                cleaned_df = cleaned_df[
                    (cleaned_df["systolic"] >= systolic_range[0]) & 
                    (cleaned_df["systolic"] <= systolic_range[1]) &
                    (cleaned_df["diastolic"] >= diastolic_range[0]) & 
                    (cleaned_df["diastolic"] <= diastolic_range[1])
                ]
                
                # 确保收缩压大于舒张压
                cleaned_df = cleaned_df[cleaned_df["systolic"] > cleaned_df["diastolic"]]
        
        else:
            # 对于其他类型的数据，应用通用的范围过滤
            value_range = data_config.get("range", None)
            if value_range and len(df.columns) == 1:
                value_col = df.columns[0]
                cleaned_df = cleaned_df[
                    (cleaned_df[value_col] >= value_range[0]) & 
                    (cleaned_df[value_col] <= value_range[1])
                ]
        
        # 移除重复的时间戳，保留最后一个值
        cleaned_df = cleaned_df[~cleaned_df.index.duplicated(keep='last')]
        
        # 检查数据点数量是否达到最小要求
        min_points = self.default_params["min_data_points"]
        if len(cleaned_df) < min_points:
            logger.warning(f"清洗后的数据点数量 {len(cleaned_df)} 少于最小要求 {min_points}")
        
        return cleaned_df
    
    def _remove_outliers(self, df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
        """
        移除异常值
        
        Args:
            df: 输入DataFrame
            threshold: 标准差阈值
            
        Returns:
            处理后的DataFrame
        """
        # 使用Z-score方法识别异常值
        z_scores = np.abs((df - df.mean()) / df.std())
        # 保留Z-score小于阈值的数据点
        filtered_df = df[(z_scores < threshold).all(axis=1)]
        
        outliers_removed = len(df) - len(filtered_df)
        if outliers_removed > 0:
            logger.info(f"移除了 {outliers_removed} 个异常值")
        
        return filtered_df
    
    def _fill_missing_values(self, df: pd.DataFrame, method: str = "linear") -> pd.DataFrame:
        """
        填充缺失值
        
        Args:
            df: 输入DataFrame
            method: 填充方法 ('linear', 'ffill', 'bfill', 'mean')
            
        Returns:
            处理后的DataFrame
        """
        filled_df = df.copy()
        
        if method == "linear":
            # 线性插值
            filled_df = filled_df.interpolate(method='linear')
        elif method == "ffill":
            # 前向填充
            filled_df = filled_df.ffill()
        elif method == "bfill":
            # 后向填充
            filled_df = filled_df.bfill()
        elif method == "mean":
            # 使用列均值填充
            filled_df = filled_df.fillna(filled_df.mean())
        else:
            logger.warning(f"未知的填充方法: {method}，使用线性插值")
            filled_df = filled_df.interpolate(method='linear')
        
        # 处理边界处的缺失值
        filled_df = filled_df.fillna(method='bfill').fillna(method='ffill')
        
        return filled_df
    
    def _resample_data(self, df: pd.DataFrame, freq: str = "1H") -> pd.DataFrame:
        """
        重采样时间序列数据
        
        Args:
            df: 输入DataFrame
            freq: 重采样频率
            
        Returns:
            重采样后的DataFrame
        """
        # 重采样，使用均值作为聚合函数
        resampled_df = df.resample(freq).mean()
        
        logger.info(f"数据从 {len(df)} 个时间点重采样到 {len(resampled_df)} 个时间点，频率: {freq}")
        
        return resampled_df
    
    def _calculate_time_series_features(self, df: pd.DataFrame, data_type: str) -> Dict:
        """
        计算时间序列特征
        
        Args:
            df: 输入DataFrame
            data_type: 数据类型
            
        Returns:
            特征字典
        """
        features = {}
        
        # 基本统计特征
        features["statistics"] = {
            "mean": df.mean().to_dict(),
            "median": df.median().to_dict(),
            "std": df.std().to_dict(),
            "min": df.min().to_dict(),
            "max": df.max().to_dict(),
            "count": df.count().to_dict()
        }
        
        # 趋势特征
        # 计算线性趋势斜率
        for col in df.columns:
            # 使用线性回归计算趋势
            try:
                from scipy import stats
                x = np.arange(len(df))
                slope, _, r_value, _, _ = stats.linregress(x, df[col].values)
                features[f"{col}_trend"] = {
                    "slope": slope,
                    "r_squared": r_value ** 2
                }
            except Exception as e:
                logger.warning(f"计算 {col} 的趋势特征失败: {e}")
        
        # 波动性特征
        features["volatility"] = {
            "rolling_std_3": df.rolling(window=3).std().mean().to_dict() if len(df) >= 3 else {},
            "coefficient_of_variation": (df.std() / df.mean()).to_dict()
        }
        
        # 数据类型特定特征
        if data_type == "blood_pressure" and "systolic" in df.columns and "diastolic" in df.columns:
            # 计算脉压差
            pulse_pressure = df["systolic"] - df["diastolic"]
            features["pulse_pressure"] = {
                "mean": pulse_pressure.mean(),
                "std": pulse_pressure.std()
            }
            
            # 计算平均动脉压 (MAP)
            mean_arterial_pressure = df["diastolic"] + (pulse_pressure / 3)
            features["mean_arterial_pressure"] = {
                "mean": mean_arterial_pressure.mean(),
                "std": mean_arterial_pressure.std()
            }
        
        return features
    
    def _assess_data_quality(self, df: pd.DataFrame, data_config: Dict) -> Dict:
        """
        评估数据质量
        
        Args:
            df: 处理后的DataFrame
            data_config: 数据类型配置
            
        Returns:
            质量评估结果
        """
        quality = {
            "data_points_count": len(df),
            "time_span_hours": (df.index.max() - df.index.min()).total_seconds() / 3600 if len(df) > 0 else 0,
            "missing_percentage": 0.0,  # 由于已经填充，这里假设为0
            "data_completeness": "good" if len(df) > self.default_params["min_data_points"] else "insufficient"
        }
        
        # 评估数据范围的合理性
        if "range" in data_config and len(df.columns) == 1:
            value_col = df.columns[0]
            normal_range = data_config["range"]
            values_in_range = ((df[value_col] >= normal_range[0]) & 
                              (df[value_col] <= normal_range[1])).mean() * 100
            quality["normal_range_percentage"] = values_in_range
        
        # 评估数据的时间分布均匀性
        if len(df) > 1:
            # 计算时间间隔的标准差
            time_diffs = df.index.to_series().diff().dropna()
            avg_interval = time_diffs.mean()
            std_interval = time_diffs.std()
            # 计算变异系数（标准差/均值）作为均匀性指标
            cv_interval = std_interval / avg_interval if avg_interval.total_seconds() > 0 else 0
            quality["temporal_uniformity"] = {
                "average_interval_seconds": avg_interval.total_seconds(),
                "interval_cv": cv_interval
            }
        
        return quality
    
    def detect_anomalies(self, df: pd.DataFrame, method: str = "zscore", 
                        params: Optional[Dict] = None) -> pd.DataFrame:
        """
        检测时间序列中的异常
        
        Args:
            df: 输入DataFrame
            method: 检测方法 ('zscore', 'iqr')
            params: 方法参数
            
        Returns:
            标记了异常的数据框
        """
        if params is None:
            params = {}
        
        result_df = df.copy()
        
        for col in df.columns:
            if method == "zscore":
                # Z-score方法
                threshold = params.get("threshold", 3.0)
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                result_df[f"{col}_is_anomaly"] = z_scores > threshold
            
            elif method == "iqr":
                # IQR方法（四分位距）
                factor = params.get("factor", 1.5)
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - factor * IQR
                upper_bound = Q3 + factor * IQR
                result_df[f"{col}_is_anomaly"] = (df[col] < lower_bound) | (df[col] > upper_bound)
            
            else:
                logger.warning(f"未知的异常检测方法: {method}")
        
        return result_df