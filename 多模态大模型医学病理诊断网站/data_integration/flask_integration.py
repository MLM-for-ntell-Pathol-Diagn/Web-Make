"""
Flask集成模块

该模块提供将数据集成底座功能集成到Flask应用的工具和接口，包括API蓝图、路由和请求处理函数。
"""
import os
import json
import logging
from flask import Blueprint, request, jsonify, current_app, send_file
from flask_restful import Api, Resource, reqparse
from werkzeug.utils import secure_filename
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 导入数据集成模块的核心功能
from . import DataIntegrationAPI, get_api
from .uploaders import BatchUploadTask

# 创建Flask蓝图
data_integration_bp = Blueprint('data_integration', __name__, url_prefix='/api/data')
data_api = Api(data_integration_bp)

# API请求解析器
image_upload_parser = reqparse.RequestParser()
image_upload_parser.add_argument('metadata', type=str, location='form', help='图像元数据（JSON字符串）')
image_upload_parser.add_argument('patient_id', type=str, location='form', help='患者ID')
image_upload_parser.add_argument('study_id', type=str, location='form', help='研究ID')
image_upload_parser.add_argument('image_type', type=str, location='form', help='图像类型')

# 文档上传解析器
document_upload_parser = reqparse.RequestParser()
document_upload_parser.add_argument('metadata', type=str, location='form', help='文档元数据（JSON字符串）')
document_upload_parser.add_argument('patient_id', type=str, location='form', help='患者ID')
document_upload_parser.add_argument('document_type', type=str, location='form', help='文档类型')

# 搜索解析器
search_parser = reqparse.RequestParser()
search_parser.add_argument('query', type=str, required=True, help='搜索查询')
search_parser.add_argument('modalities', type=str, help='模态列表（逗号分隔）')
search_parser.add_argument('filters', type=str, help='过滤条件（JSON字符串）')
search_parser.add_argument('limit', type=int, default=50, help='结果数量限制')

# 批量上传解析器
batch_upload_parser = reqparse.RequestParser()
batch_upload_parser.add_argument('file_paths', type=str, required=True, help='文件路径列表（JSON字符串）')
batch_upload_parser.add_argument('metadata_list', type=str, help='元数据列表（JSON字符串）')
batch_upload_parser.add_argument('priority', type=str, default='medium', help='任务优先级')


class ImageUploadResource(Resource):
    """
    图像上传API资源
    """
    def post(self):
        """
        上传单个图像
        """
        try:
            # 获取上传的文件
            if 'file' not in request.files:
                return jsonify({'error': '未提供文件'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': '未选择文件'}), 400
            
            # 解析其他参数
            args = image_upload_parser.parse_args()
            
            # 构建元数据
            metadata = {}
            if args.metadata:
                try:
                    metadata = json.loads(args.metadata)
                except json.JSONDecodeError:
                    return jsonify({'error': '无效的元数据格式'}), 400
            
            # 添加额外的元数据字段
            for field in ['patient_id', 'study_id', 'image_type']:
                if args.get(field):
                    metadata[field] = args[field]
            
            # 保存临时文件
            temp_dir = current_app.config.get('TEMP_DIR', '/tmp')
            os.makedirs(temp_dir, exist_ok=True)
            
            filename = secure_filename(file.filename)
            temp_path = os.path.join(temp_dir, filename)
            file.save(temp_path)
            
            try:
                # 使用数据集成API上传和处理图像
                api = get_api()
                result = api.upload_and_process_image(temp_path, metadata)
                
                if result.get('success'):
                    return jsonify(result.get('data')), 201
                else:
                    return jsonify({'error': result.get('error')}), 400
            finally:
                # 清理临时文件
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception as e:
                        logger.warning(f"清理临时文件失败 {temp_path}: {e}")
                        
        except Exception as e:
            logger.error(f"图像上传失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class DocumentUploadResource(Resource):
    """
    文档上传API资源
    """
    def post(self):
        """
        上传单个文档
        """
        try:
            # 获取上传的文件
            if 'file' not in request.files:
                return jsonify({'error': '未提供文件'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': '未选择文件'}), 400
            
            # 解析其他参数
            args = document_upload_parser.parse_args()
            
            # 构建元数据
            metadata = {}
            if args.metadata:
                try:
                    metadata = json.loads(args.metadata)
                except json.JSONDecodeError:
                    return jsonify({'error': '无效的元数据格式'}), 400
            
            # 添加额外的元数据字段
            for field in ['patient_id', 'document_type']:
                if args.get(field):
                    metadata[field] = args[field]
            
            # 保存临时文件
            temp_dir = current_app.config.get('TEMP_DIR', '/tmp')
            os.makedirs(temp_dir, exist_ok=True)
            
            filename = secure_filename(file.filename)
            temp_path = os.path.join(temp_dir, filename)
            file.save(temp_path)
            
            try:
                # 使用数据集成API上传和处理文档
                api = get_api()
                result = api.upload_and_process_document(temp_path, metadata)
                
                if result.get('success'):
                    return jsonify(result.get('data')), 201
                else:
                    return jsonify({'error': result.get('error')}), 400
            finally:
                # 清理临时文件
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception as e:
                        logger.warning(f"清理临时文件失败 {temp_path}: {e}")
                        
        except Exception as e:
            logger.error(f"文档上传失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class SearchResource(Resource):
    """
    多模态搜索API资源
    """
    def get(self):
        """
        执行搜索查询
        """
        try:
            # 解析查询参数
            args = search_parser.parse_args()
            
            # 处理模态列表
            modalities = None
            if args.modalities:
                modalities = [m.strip() for m in args.modalities.split(',')]
            
            # 处理过滤条件
            filters = None
            if args.filters:
                try:
                    filters = json.loads(args.filters)
                except json.JSONDecodeError:
                    return jsonify({'error': '无效的过滤条件格式'}), 400
            
            # 执行搜索
            api = get_api()
            results = api.search_multimodal(
                query=args.query,
                modalities=modalities,
                filters=filters
            )
            
            return jsonify(results), 200
            
        except Exception as e:
            logger.error(f"搜索失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class PatientDataResource(Resource):
    """
    患者数据API资源
    """
    def get(self, patient_id):
        """
        获取患者的所有数据
        """
        try:
            # 处理模态参数
            modalities = None
            if request.args.get('modalities'):
                modalities = [m.strip() for m in request.args['modalities'].split(',')]
            
            # 获取患者数据
            api = get_api()
            results = api.get_patient_data(patient_id, modalities)
            
            return jsonify(results), 200
            
        except Exception as e:
            logger.error(f"获取患者数据失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class BatchUploadResource(Resource):
    """
    批量上传API资源
    """
    def post(self):
        """
        创建批量上传任务
        """
        try:
            # 解析请求参数
            args = batch_upload_parser.parse_args()
            
            # 解析文件路径列表
            try:
                file_paths = json.loads(args.file_paths)
                if not isinstance(file_paths, list):
                    return jsonify({'error': '文件路径必须是列表格式'}), 400
            except json.JSONDecodeError:
                return jsonify({'error': '无效的文件路径列表格式'}), 400
            
            # 解析元数据列表（如果提供）
            metadata_list = None
            if args.metadata_list:
                try:
                    metadata_list = json.loads(args.metadata_list)
                    if not isinstance(metadata_list, list):
                        return jsonify({'error': '元数据列表必须是列表格式'}), 400
                except json.JSONDecodeError:
                    return jsonify({'error': '无效的元数据列表格式'}), 400
            
            # 验证文件路径
            for path in file_paths:
                if not os.path.exists(path):
                    return jsonify({'error': f'文件不存在: {path}'}), 400
            
            # 启动批量上传任务
            api = get_api()
            result = api.start_batch_upload(
                file_paths=file_paths,
                metadata_list=metadata_list,
                priority=args.priority
            )
            
            return jsonify(result), 202
            
        except Exception as e:
            logger.error(f"批量上传任务创建失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class BatchTaskStatusResource(Resource):
    """
    批量任务状态API资源
    """
    def get(self, task_id):
        """
        获取批量任务状态
        """
        try:
            api = get_api()
            status = api.get_batch_task_status(task_id)
            
            if status.get('success'):
                return jsonify(status), 200
            else:
                return jsonify({'error': status.get('error')}), 404
                
        except Exception as e:
            logger.error(f"获取任务状态失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class MetadataUpdateResource(Resource):
    """
    元数据更新API资源
    """
    def put(self, entity_type, entity_id):
        """
        更新实体元数据
        """
        try:
            # 获取更新数据
            update_data = request.get_json()
            if not isinstance(update_data, dict):
                return jsonify({'error': '请求体必须是JSON对象'}), 400
            
            # 更新元数据
            api = get_api()
            result = api.update_metadata(entity_id, entity_type, update_data)
            
            return jsonify(result), 200
            
        except Exception as e:
            logger.error(f"更新元数据失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class ImageRetrieveResource(Resource):
    """
    图像检索API资源
    """
    def get(self, image_id):
        """
        根据ID检索图像
        """
        try:
            api = get_api()
            image_path = api.image_storage.get_image_path(image_id)
            
            if not image_path or not os.path.exists(image_path):
                return jsonify({'error': '图像不存在'}), 404
            
            # 返回图像文件
            return send_file(image_path)
            
        except Exception as e:
            logger.error(f"获取图像失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class DocumentRetrieveResource(Resource):
    """
    文档检索API资源
    """
    def get(self, document_id):
        """
        根据ID检索文档
        """
        try:
            api = get_api()
            document_path = api.document_storage.get_document_path(document_id)
            
            if not document_path or not os.path.exists(document_path):
                return jsonify({'error': '文档不存在'}), 404
            
            # 返回文档文件
            return send_file(document_path)
            
        except Exception as e:
            logger.error(f"获取文档失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


class SystemStatusResource(Resource):
    """
    系统状态API资源
    """
    def get(self):
        """
        获取系统状态信息
        """
        try:
            api = get_api()
            
            # 获取存储统计
            image_stats = api.image_storage.get_storage_stats()
            document_stats = api.document_storage.get_storage_stats()
            
            # 构建状态信息
            status = {
                'status': 'operational',
                'version': '1.0.0',
                'storage': {
                    'images': image_stats,
                    'documents': document_stats
                },
                'batch_tasks': {
                    'active': len(api.batch_manager.get_active_tasks()),
                    'queue': len(api.batch_manager.get_queued_tasks())
                }
            }
            
            return jsonify(status), 200
            
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return jsonify({'error': f'服务器内部错误: {str(e)}'}), 500


# 注册API路由
data_api.add_resource(ImageUploadResource, '/images/upload')
data_api.add_resource(DocumentUploadResource, '/documents/upload')
data_api.add_resource(SearchResource, '/search')
data_api.add_resource(PatientDataResource, '/patients/<patient_id>')
data_api.add_resource(BatchUploadResource, '/batch/upload')
data_api.add_resource(BatchTaskStatusResource, '/batch/tasks/<task_id>')
data_api.add_resource(MetadataUpdateResource, '/metadata/<entity_type>/<entity_id>')
data_api.add_resource(ImageRetrieveResource, '/images/<image_id>')
data_api.add_resource(DocumentRetrieveResource, '/documents/<document_id>')
data_api.add_resource(SystemStatusResource, '/status')


def register_data_integration(app, config=None):
    """
    将数据集成模块注册到Flask应用
    
    Args:
        app: Flask应用实例
        config: 数据集成模块配置
    """
    # 注册蓝图
    app.register_blueprint(data_integration_bp)
    
    # 设置必要的配置
    if not app.config.get('TEMP_DIR'):
        app.config['TEMP_DIR'] = os.path.join(app.root_path, 'temp')
    
    # 确保临时目录存在
    os.makedirs(app.config['TEMP_DIR'], exist_ok=True)
    
    # 初始化数据集成API
    api = get_api(config)
    
    # 在应用上下文中存储API实例
    app.data_integration_api = api
    
    # 添加应用启动和关闭处理
    @app.before_first_request
    def before_first_request():
        """应用首次请求前的初始化"""
        logger.info("数据集成模块已初始化")
    
    @app.teardown_appcontext
    def teardown_appcontext(exception):
        """应用上下文销毁时的清理"""
        if exception:
            logger.error(f"应用上下文销毁时发生异常: {exception}")
    
    logger.info("数据集成模块已成功注册到Flask应用")


def create_example_application(config=None):
    """
    创建一个包含数据集成模块的示例Flask应用
    
    Args:
        config: 应用配置
        
    Returns:
        Flask应用实例
    """
    from flask import Flask
    
    # 创建Flask应用
    app = Flask(__name__)
    
    # 设置配置
    if config:
        app.config.update(config)
    
    # 设置基本配置
    app.config.setdefault('SECRET_KEY', 'dev_key_for_testing')
    app.config.setdefault('TEMP_DIR', os.path.join(os.path.dirname(__file__), 'temp'))
    
    # 注册数据集成模块
    register_data_integration(app)
    
    # 添加基本路由
    @app.route('/')
    def index():
        return jsonify({
            'message': '多源数据集成底座API服务',
            'version': '1.0.0',
            'endpoints': {
                'image_upload': '/api/data/images/upload',
                'document_upload': '/api/data/documents/upload',
                'search': '/api/data/search',
                'patient_data': '/api/data/patients/<patient_id>',
                'batch_upload': '/api/data/batch/upload',
                'batch_task_status': '/api/data/batch/tasks/<task_id>',
                'system_status': '/api/data/status'
            }
        })
    
    return app


# 命令行入口
if __name__ == '__main__':
    # 创建示例应用并运行
    app = create_example_application()
    app.run(debug=True, host='0.0.0.0', port=5000)