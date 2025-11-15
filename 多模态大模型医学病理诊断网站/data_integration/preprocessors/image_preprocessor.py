"""图像预处理模块"""
import os
import numpy as np
from typing import Dict, Optional, Tuple, Union
from PIL import Image
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ImageQualityEnhancer:
    """
    图像质量提升处理器
    提供去噪、裁剪、虚拟染色、色彩归一化等功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化图像质量提升处理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        # 设置默认参数
        self.default_params = {
            "denoise_strength": 1.0,
            "crop_margin": 0.05,
            "normalize_method": "histogram",
            "virtual_stain_intensity": 1.2
        }
        # 更新默认参数
        self.default_params.update(self.config)
    
    def enhance_image(self, image_path: str, output_path: Optional[str] = None, 
                     operations: Optional[Dict] = None) -> str:
        """
        执行图像增强处理
        
        Args:
            image_path: 输入图像路径
            output_path: 输出图像路径
            operations: 要执行的操作及其参数
            
        Returns:
            处理后的图像路径
        """
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"图像文件不存在: {image_path}")
        
        # 设置默认操作
        if operations is None:
            operations = {
                "denoise": True,
                "normalize": True,
                "crop": False,
                "virtual_stain": False
            }
        
        # 确保输出路径有效
        if output_path is None:
            base_name = os.path.basename(image_path)
            name, ext = os.path.splitext(base_name)
            output_path = os.path.join(os.path.dirname(image_path), f"{name}_enhanced{ext}")
        
        try:
            # 读取图像
            image = Image.open(image_path)
            image_array = np.array(image)
            
            # 执行指定的操作
            if operations.get("denoise", False):
                image_array = self._denoise_image(image_array, 
                                               operations.get("denoise_strength", 
                                                             self.default_params["denoise_strength"]))
            
            if operations.get("normalize", False):
                method = operations.get("normalize_method", self.default_params["normalize_method"])
                image_array = self._normalize_color(image_array, method)
            
            if operations.get("crop", False):
                margin = operations.get("crop_margin", self.default_params["crop_margin"])
                image_array = self._crop_image(image_array, margin)
            
            if operations.get("virtual_stain", False):
                stain_type = operations.get("stain_type", "ihc")
                intensity = operations.get("stain_intensity", 
                                          self.default_params["virtual_stain_intensity"])
                image_array = self._virtual_staining(image_array, stain_type, intensity)
            
            # 保存处理后的图像
            result_image = Image.fromarray(image_array)
            result_image.save(output_path)
            
            logger.info(f"图像增强完成: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"图像增强失败: {e}")
            raise
    
    def _denoise_image(self, image_array: np.ndarray, strength: float = 1.0) -> np.ndarray:
        """
        图像去噪
        
        Args:
            image_array: 图像数组
            strength: 去噪强度
            
        Returns:
            去噪后的图像数组
        """
        # 这是一个简化实现
        # 实际项目中可以使用更复杂的去噪算法，如高斯滤波、中值滤波或小波去噪
        logger.info(f"执行图像去噪，强度: {strength}")
        
        # 模拟去噪操作
        # 在实际应用中，这里应该使用OpenCV等库进行真实的去噪处理
        if len(image_array.shape) == 3:  # 彩色图像
            # 简单的平滑处理模拟
            from scipy import ndimage
            sigma = 0.5 * strength
            return ndimage.gaussian_filter(image_array, sigma=(sigma, sigma, 0))
        else:  # 灰度图像
            from scipy import ndimage
            sigma = 0.5 * strength
            return ndimage.gaussian_filter(image_array, sigma=sigma)
    
    def _normalize_color(self, image_array: np.ndarray, method: str = "histogram") -> np.ndarray:
        """
        色彩归一化
        
        Args:
            image_array: 图像数组
            method: 归一化方法 ('histogram', 'minmax', 'zscore')
            
        Returns:
            归一化后的图像数组
        """
        logger.info(f"执行色彩归一化，方法: {method}")
        
        # 确保图像数组为float类型
        img_float = image_array.astype(float)
        
        if method == "histogram":
            # 直方图均衡化
            if len(img_float.shape) == 3:  # 彩色图像
                # 转换到YUV色彩空间进行亮度通道均衡化
                from skimage import color
                img_yuv = color.rgb2yuv(img_float / 255.0)
                # 对Y通道进行直方图均衡化
                from skimage import exposure
                img_yuv[:, :, 0] = exposure.equalize_hist(img_yuv[:, :, 0])
                # 转回RGB
                result = color.yuv2rgb(img_yuv)
                return (result * 255).astype(np.uint8)
            else:  # 灰度图像
                from skimage import exposure
                result = exposure.equalize_hist(img_float / 255.0)
                return (result * 255).astype(np.uint8)
        
        elif method == "minmax":
            # 最小-最大归一化
            min_vals = img_float.min(axis=(0, 1))
            max_vals = img_float.max(axis=(0, 1))
            # 避免除零错误
            range_vals = np.maximum(max_vals - min_vals, 1)
            
            if len(img_float.shape) == 3:  # 彩色图像
                for i in range(3):
                    img_float[:, :, i] = 255 * (img_float[:, :, i] - min_vals[i]) / range_vals[i]
            else:  # 灰度图像
                img_float = 255 * (img_float - min_vals) / range_vals
            
            return np.clip(img_float, 0, 255).astype(np.uint8)
        
        elif method == "zscore":
            # Z-score标准化
            mean_vals = img_float.mean(axis=(0, 1))
            std_vals = img_float.std(axis=(0, 1))
            # 避免除零错误
            std_vals = np.maximum(std_vals, 1)
            
            if len(img_float.shape) == 3:  # 彩色图像
                for i in range(3):
                    img_float[:, :, i] = 127 + 50 * (img_float[:, :, i] - mean_vals[i]) / std_vals[i]
            else:  # 灰度图像
                img_float = 127 + 50 * (img_float - mean_vals) / std_vals
            
            return np.clip(img_float, 0, 255).astype(np.uint8)
        
        else:
            logger.warning(f"未知的归一化方法: {method}，使用默认的直方图均衡化")
            return self._normalize_color(image_array, method="histogram")
    
    def _crop_image(self, image_array: np.ndarray, margin_ratio: float = 0.05) -> np.ndarray:
        """
        图像裁剪
        
        Args:
            image_array: 图像数组
            margin_ratio: 裁剪边距比例
            
        Returns:
            裁剪后的图像数组
        """
        logger.info(f"执行图像裁剪，边距比例: {margin_ratio}")
        
        height, width = image_array.shape[:2]
        margin_h = int(height * margin_ratio)
        margin_w = int(width * margin_ratio)
        
        # 确保边距有效
        margin_h = max(0, min(margin_h, height // 2))
        margin_w = max(0, min(margin_w, width // 2))
        
        return image_array[margin_h:height-margin_h, margin_w:width-margin_w]
    
    def _virtual_staining(self, image_array: np.ndarray, stain_type: str = "ihc", 
                         intensity: float = 1.2) -> np.ndarray:
        """
        虚拟染色
        
        Args:
            image_array: 图像数组
            stain_type: 染色类型 ('ihc', 'he')
            intensity: 染色强度
            
        Returns:
            虚拟染色后的图像数组
        """
        logger.info(f"执行虚拟染色，类型: {stain_type}，强度: {intensity}")
        
        # 确保图像为RGB格式
        if len(image_array.shape) != 3:
            # 灰度转RGB
            from skimage import color
            img_rgb = color.gray2rgb(image_array)
        else:
            img_rgb = image_array.copy() / 255.0
        
        if stain_type.lower() == "ihc":
            # 模拟IHC染色（棕色染色）
            # 增强红色和黄色通道
            img_rgb[:, :, 0] = np.clip(img_rgb[:, :, 0] * intensity, 0, 1)
            img_rgb[:, :, 1] = np.clip(img_rgb[:, :, 1] * 0.8, 0, 1)
            img_rgb[:, :, 2] = np.clip(img_rgb[:, :, 2] * 0.6, 0, 1)
        
        elif stain_type.lower() == "he":
            # 模拟HE染色（苏木精-伊红，蓝色和红色）
            # 增强蓝色通道（核）和红色通道（细胞质）
            img_rgb[:, :, 0] = np.clip(img_rgb[:, :, 0] * 0.9, 0, 1)
            img_rgb[:, :, 1] = np.clip(img_rgb[:, :, 1] * 0.8, 0, 1)
            img_rgb[:, :, 2] = np.clip(img_rgb[:, :, 2] * intensity, 0, 1)
        
        else:
            logger.warning(f"未知的染色类型: {stain_type}，使用默认的IHC染色")
            return self._virtual_staining(image_array, stain_type="ihc", intensity=intensity)
        
        return (img_rgb * 255).astype(np.uint8)
    
    def batch_process(self, image_paths: List[str], output_dir: Optional[str] = None, 
                     operations: Optional[Dict] = None) -> List[str]:
        """
        批量处理图像
        
        Args:
            image_paths: 图像路径列表
            output_dir: 输出目录
            operations: 要执行的操作
            
        Returns:
            处理后的图像路径列表
        """
        result_paths = []
        
        for img_path in image_paths:
            try:
                # 确定输出路径
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    base_name = os.path.basename(img_path)
                    out_path = os.path.join(output_dir, base_name)
                else:
                    out_path = None
                
                # 处理图像
                result_path = self.enhance_image(img_path, out_path, operations)
                result_paths.append(result_path)
                
            except Exception as e:
                logger.error(f"处理图像失败 {img_path}: {e}")
                # 可以选择继续处理下一张图像或抛出异常
                # raise
        
        return result_paths