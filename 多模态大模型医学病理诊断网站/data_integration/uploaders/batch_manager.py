"""批量上传管理模块"""
import os
import threading
import queue
import time
from typing import Dict, List, Optional, BinaryIO, Callable
import uuid
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchUploadTask:
    """批量上传任务"""
    
    def __init__(self, file_obj: BinaryIO, metadata: Optional[Dict] = None, task_type: str = "image"):
        """
        初始化上传任务
        
        Args:
            file_obj: 文件对象
            metadata: 元数据信息
            task_type: 任务类型
        """
        self.task_id = str(uuid.uuid4())
        self.file_obj = file_obj
        self.metadata = metadata or {}
        self.task_type = task_type
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.status = "pending"
        self.result: Optional[Dict] = None
        self.error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """
        转换为字典格式
        
        Returns:
            任务信息字典
        """
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "metadata": self.metadata,
            "result": self.result,
            "error": self.error
        }


class BatchUploadManager:
    """
    批量上传管理器
    提供图像批量上传和队列调度功能
    """
    
    def __init__(self, max_workers: int = 4, upload_handler: Optional[Callable] = None):
        """
        初始化批量上传管理器
        
        Args:
            max_workers: 最大工作线程数
            upload_handler: 上传处理函数
        """
        self.task_queue = queue.Queue()
        self.max_workers = max_workers
        self.upload_handler = upload_handler
        self.workers: List[threading.Thread] = []
        self.tasks: Dict[str, BatchUploadTask] = {}
        self.running = False
        self.lock = threading.Lock()
    
    def start(self):
        """
        启动批量上传管理器
        """
        if self.running:
            logger.warning("批量上传管理器已经在运行中")
            return
        
        self.running = True
        # 创建并启动工作线程
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker_thread, name=f"UploadWorker-{i}")
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"批量上传管理器已启动，工作线程数: {self.max_workers}")
    
    def stop(self):
        """
        停止批量上传管理器
        """
        if not self.running:
            logger.warning("批量上传管理器未在运行")
            return
        
        self.running = False
        # 等待所有工作线程结束
        for worker in self.workers:
            worker.join(timeout=5)
        
        self.workers.clear()
        logger.info("批量上传管理器已停止")
    
    def add_task(self, file_obj: BinaryIO, metadata: Optional[Dict] = None, task_type: str = "image") -> str:
        """
        添加上传任务
        
        Args:
            file_obj: 文件对象
            metadata: 元数据信息
            task_type: 任务类型
            
        Returns:
            任务ID
        """
        task = BatchUploadTask(file_obj, metadata, task_type)
        
        # 添加任务到队列和任务字典
        with self.lock:
            self.task_queue.put(task)
            self.tasks[task.task_id] = task
        
        logger.info(f"添加任务到队列: {task.task_id}")
        return task.task_id
    
    def add_batch_tasks(self, tasks_data: List[Dict]) -> List[str]:
        """
        批量添加上传任务
        
        Args:
            tasks_data: 任务数据列表，每项包含file_obj和metadata
            
        Returns:
            任务ID列表
        """
        task_ids = []
        
        with self.lock:
            for task_data in tasks_data:
                file_obj = task_data.get("file_obj")
                metadata = task_data.get("metadata", {})
                task_type = task_data.get("task_type", "image")
                
                task = BatchUploadTask(file_obj, metadata, task_type)
                self.task_queue.put(task)
                self.tasks[task.task_id] = task
                task_ids.append(task.task_id)
        
        logger.info(f"批量添加任务完成，共添加 {len(task_ids)} 个任务")
        return task_ids
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态信息
        """
        with self.lock:
            task = self.tasks.get(task_id)
            if task:
                return task.to_dict()
        return None
    
    def get_all_tasks_status(self) -> List[Dict]:
        """
        获取所有任务状态
        
        Returns:
            所有任务的状态信息列表
        """
        with self.lock:
            return [task.to_dict() for task in self.tasks.values()]
    
    def get_pending_tasks_count(self) -> int:
        """
        获取待处理任务数量
        
        Returns:
            待处理任务数量
        """
        return self.task_queue.qsize()
    
    def _worker_thread(self):
        """
        工作线程函数
        """
        while self.running:
            try:
                # 从队列获取任务，设置超时以便定期检查running状态
                task = self.task_queue.get(timeout=1)
                
                # 更新任务状态
                with self.lock:
                    task.status = "processing"
                    task.started_at = datetime.now()
                
                logger.info(f"开始处理任务: {task.task_id}")
                
                try:
                    # 调用上传处理函数
                    if self.upload_handler:
                        result = self.upload_handler(task.file_obj, task.metadata)
                        task.result = result
                        task.status = "completed"
                        logger.info(f"任务处理完成: {task.task_id}")
                    else:
                        # 模拟上传处理
                        time.sleep(2)  # 模拟处理时间
                        task.result = {"message": "模拟上传成功", "task_id": task.task_id}
                        task.status = "completed"
                        logger.info(f"任务模拟处理完成: {task.task_id}")
                except Exception as e:
                    # 处理任务执行异常
                    error_msg = str(e)
                    with self.lock:
                        task.status = "failed"
                        task.error = error_msg
                    logger.error(f"任务处理失败: {task.task_id}, 错误: {error_msg}")
                finally:
                    # 更新任务完成时间
                    with self.lock:
                        task.completed_at = datetime.now()
                    
                    # 标记任务完成
                    self.task_queue.task_done()
                    
                    # 如果任务失败，根据配置可以选择重试
                    if task.status == "failed" and task.metadata.get("auto_retry", False):
                        retry_count = task.metadata.get("retry_count", 0)
                        max_retries = task.metadata.get("max_retries", 3)
                        
                        if retry_count < max_retries:
                            task.metadata["retry_count"] = retry_count + 1
                            with self.lock:
                                self.task_queue.put(task)
                                task.status = "pending"
                                task.started_at = None
                                task.completed_at = None
                            logger.info(f"任务将重试: {task.task_id}, 重试次数: {retry_count + 1}")
            except queue.Empty:
                # 队列为空，继续循环
                continue
            except Exception as e:
                logger.error(f"工作线程异常: {e}")
    
    def pause_task(self, task_id: str) -> bool:
        """
        暂停任务（需要额外实现任务优先级或暂停队列）
        注：当前实现为简化版，实际项目中可能需要更复杂的暂停机制
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否暂停成功
        """
        # 注意：这是一个简化实现
        # 实际项目中可能需要重新设计队列系统以支持任务暂停
        logger.warning("任务暂停功能尚未完全实现")
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """
        恢复任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否恢复成功
        """
        # 注意：这是一个简化实现
        logger.warning("任务恢复功能尚未完全实现")
        return False
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否取消成功
        """
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                # 只能取消处于pending状态的任务
                if task.status == "pending":
                    # 注意：从队列中移除特定任务比较复杂，这里只是标记为取消
                    # 实际项目中可能需要使用优先级队列或其他机制
                    task.status = "cancelled"
                    logger.info(f"任务已取消: {task_id}")
                    return True
        return False