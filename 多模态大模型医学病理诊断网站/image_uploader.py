"""病理图像上传器模块"""
import os
import uuid
from typing import Dict, Optional, List, BinaryIO
from datetime import datetime


class PathologyImageUploader:
    """
    病理图像上传器
    支持HE/IHC切片、原始切片以及扫描仪/显微镜设备数据上传
    """
    
    def __init__(self, upload_dir: str = "uploads"):
        """
        初始化病理图像上传器
        
        Args:
            upload_dir: 上传文件保存目录
        """
        self.upload_dir = upload_dir
        os.makedirs(upload_dir, exist_ok=True)
        # 创建不同类型图像的子目录
        self.subdirs = {
            "he": os.path.join(upload_dir, "he_slides"),
            "ihc": os.path.join(upload_dir, "ihc_slides"),
            "raw": os.path.join(upload_dir, "raw_slides"),
            "scanner": os.path.join(upload_dir, "scanner_data")
        }
        for subdir in self.subdirs.values():
            os.makedirs(subdir, exist_ok=True)
    
    def upload_he_slide(self, file_obj: BinaryIO, metadata: Optional[Dict] = None) -> Dict:
        """
        上传HE切片图像
        
        Args:
            file_obj: 文件对象
            metadata: 元数据信息
            
        Returns:
            上传结果包含文件路径、ID等信息
        """
        return self._upload_slide("he", file_obj, metadata)
    
    def upload_ihc_slide(self, file_obj: BinaryIO, metadata: Optional[Dict] = None) -> Dict:
        """
        上传IHC切片图像
        
        Args:
            file_obj: 文件对象
            metadata: 元数据信息
            
        Returns:
            上传结果包含文件路径、ID等信息
        """
        return self._upload_slide("ihc", file_obj, metadata)
    
    def upload_raw_slide(self, file_obj: BinaryIO, metadata: Optional[Dict] = None) -> Dict:
        """
        上传原始切片图像
        
        Args:
            file_obj: 文件对象
            metadata: 元数据信息
            
        Returns:
            上传结果包含文件路径、ID等信息
        """
        return self._upload_slide("raw", file_obj, metadata)
    
    def upload_scanner_data(self, file_obj: BinaryIO, device_info: Dict, metadata: Optional[Dict] = None) -> Dict:
        """
        上传扫描仪/显微镜设备数据
        
        Args:
            file_obj: 文件对象
            device_info: 设备信息
            metadata: 元数据信息
            
        Returns:
            上传结果包含文件路径、ID等信息
        """
        if metadata is None:
            metadata = {}
        metadata['device_info'] = device_info
        return self._upload_slide("scanner", file_obj, metadata)
    
    def _upload_slide(self, slide_type: str, file_obj: BinaryIO, metadata: Optional[Dict] = None) -> Dict:
        """
        内部方法：处理幻灯片上传
        
        Args:
            slide_type: 切片类型
            file_obj: 文件对象
            metadata: 元数据信息
            
        Returns:
            上传结果
        """
        # 生成唯一ID和文件名
        slide_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 获取文件扩展名
        original_filename = getattr(file_obj, 'filename', 'unknown')
        ext = os.path.splitext(original_filename)[1]
        if not ext:
            ext = ".svs"  # 默认使用.svs格式
        
        # 构建保存路径
        filename = f"{slide_type}_{timestamp}_{slide_id}{ext}"
        filepath = os.path.join(self.subdirs[slide_type], filename)
        
        # 保存文件
        with open(filepath, 'wb') as f:
            f.write(file_obj.read())
        
        # 准备返回结果
        result = {
            "slide_id": slide_id,
            "slide_type": slide_type,
            "filename": filename,
            "filepath": filepath,
            "upload_time": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        
        # 这里可以添加数据库记录逻辑
        self._record_upload(result)
        
        return result
    
    def _record_upload(self, upload_info: Dict) -> None:
        """
        记录上传信息到数据库（模拟实现）
        
        Args:
            upload_info: 上传信息
        """
        # 实际项目中这里应该将数据写入数据库
        print(f"记录上传信息: {upload_info['slide_id']} - {upload_info['filename']}")
    
    def get_supported_formats(self) -> List[str]:
        """
        获取支持的文件格式
        
        Returns:
            支持的文件格式列表
        """
        return [".svs", ".tif", ".tiff", ".ndpi", ".vms", ".vmu", ".scn", ".mrxs"]
    
    def validate_image(self, file_obj: BinaryIO) -> bool:
        """
        验证图像文件是否有效
        
        Args:
            file_obj: 文件对象
            
        Returns:
            是否为有效图像
        """
        # 检查文件扩展名
        original_filename = getattr(file_obj, 'filename', 'unknown').lower()
        ext = os.path.splitext(original_filename)[1]
        
        # 重置文件指针
        current_pos = file_obj.tell()
        file_obj.seek(0)
        
        # 这里可以添加更复杂的验证逻辑，如检查文件头
        is_valid = ext in self.get_supported_formats()
        
        # 恢复文件指针
        file_obj.seek(current_pos)
        
        return is_valid