# 多源数据集成底座

## 模块概述

多源数据集成底座是一个功能强大的数据处理框架，专为医学病理诊断网站设计，支持多模态医学数据的上传、预处理、存储和检索。该模块提供了完整的数据生命周期管理，从数据获取到最终分析使用，确保数据的一致性、安全性和可用性。

### 主要功能

- **多渠道数据上传接口**：支持图像、文本、结构化数据等多类型医学数据的上传
- **医疗系统集成**：支持与HIS、EMR、PACS、LIS等医疗系统的对接
- **批量数据管理**：支持大批量医学数据的并行处理和任务调度
- **多模态数据预处理**：包含图像质量提升、文本标准化、时间序列处理等功能
- **统一存储管理**：提供图像、文档的高效存储和元数据索引
- **跨模态检索引擎**：支持基于患者ID、研究ID、关键词等多维度的数据检索
- **Flask框架集成**：提供RESTful API接口，方便与Web应用集成

## 安装与配置

### 环境要求

- Python 3.8+
- Flask 2.0+
- Pillow 9.0+
- numpy 1.20+
- pandas 1.3+
- tqdm 4.62+

### 安装步骤

1. 克隆项目代码：

```bash
git clone <repository-url>
cd <project-directory>/多模态大模型医学病理诊断网站
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

3. 创建配置文件：

```bash
python -m data_integration.config --create-config config.json
```

4. 根据实际环境修改配置文件

## 目录结构

```
data_integration/
├── __init__.py           # 模块主入口
├── config.py             # 配置管理
├── flask_integration.py  # Flask集成
├── uploaders/            # 数据上传模块
│   ├── __init__.py
│   ├── image_uploader.py
│   ├── system_integration.py
│   └── batch_manager.py
├── preprocessors/        # 数据预处理模块
│   ├── __init__.py
│   ├── image_preprocessor.py
│   ├── text_preprocessor.py
│   └── time_series_processor.py
└── storage/              # 存储与检索模块
    ├── __init__.py
    ├── image_storage.py
    ├── document_storage.py
    ├── metadata_index.py
    ├── retrieval_engine.py
    └── storage_factory.py
```

## 配置指南

### 配置文件结构

配置文件使用JSON格式，主要包含以下几个部分：

- **基础配置**：应用名称、版本、调试模式等
- **存储配置**：存储路径、文件大小限制、索引配置等
- **上传配置**：并发控制、队列大小、超时设置等
- **预处理配置**：图像处理参数、文本处理选项等
- **Flask配置**：密钥、内容长度限制、跨域设置等
- **医疗系统配置**：各类医疗系统的连接参数

### 环境变量支持

模块支持通过环境变量覆盖配置文件中的设置：

- `MEDICAL_STORAGE_PATH`：存储基础路径
- `MEDICAL_TEMP_PATH`：临时文件路径
- `MEDICAL_DEBUG`：调试模式开关
- `MEDICAL_LOG_LEVEL`：日志级别
- `MEDICAL_SECRET_KEY`：Flask密钥

## API 参考

### 数据集成模块API

#### 初始化与配置

```python
from data_integration import DataIntegrationAPI

# 使用默认配置初始化
api = DataIntegrationAPI()

# 使用自定义配置初始化
api = DataIntegrationAPI(config_file='path/to/config.json', environment='production')
```

#### 数据上传API

##### 单张图像上传

```python
from data_integration import DataIntegrationAPI

api = DataIntegrationAPI()

# 从文件上传图像
image_id = api.upload_image(
    image_path='path/to/image.tiff',
    metadata={
        'patient_id': 'P12345',
        'study_id': 'S54321',
        'modality': 'Histology',
        'acquisition_date': '2023-04-15',
        'description': '肝脏切片病理图像'
    },
    preprocess=True
)

# 从字节数据上传图像
with open('path/to/image.tiff', 'rb') as f:
    image_data = f.read()
    image_id = api.upload_image_bytes(
        image_bytes=image_data,
        filename='liver_sample.tiff',
        metadata={...},  # 同上
        preprocess=True
    )
```

##### 批量图像上传

```python
# 创建批量上传任务
batch_id = api.create_batch_upload([
    {'image_path': 'image1.tiff', 'metadata': {...}},
    {'image_path': 'image2.tiff', 'metadata': {...}},
    {'image_path': 'image3.tiff', 'metadata': {...}}
])

# 获取任务状态
status = api.get_batch_status(batch_id)
print(f"任务状态: {status['status']}")
print(f"已完成: {status['completed_count']}/{status['total_count']}")

# 取消任务
api.cancel_batch_upload(batch_id)
```

##### 医疗系统数据获取

```python
# 获取患者信息
patient_info = api.get_patient_info('P12345')

# 获取患者病历
medical_records = api.get_medical_records('P12345', start_date='2023-01-01')

# 获取检验结果
lab_results = api.get_lab_results('P12345', test_types=['Blood', 'Biochemistry'])

# 获取影像检查
images = api.get_imaging_studies('P12345', modality='CT')
```

#### 数据检索API

```python
# 根据患者ID搜索数据
patient_data = api.search_by_patient_id('P12345')

# 根据研究ID搜索数据
study_data = api.search_by_study_id('S54321')

# 全文搜索
results = api.search_text('肝癌 肝硬化', limit=50)

# 高级结构化查询
advanced_results = api.search_structured({
    'modality': 'Histology',
    'acquisition_date_range': {'start': '2023-01-01', 'end': '2023-12-31'},
    'patient_age_range': {'min': 40, 'max': 60},
    'metadata_filters': {'tissue_type': 'Liver'}
})
```

#### 数据预处理API

```python
# 图像预处理
processed_image = api.preprocess_image(
    image_path='path/to/image.tiff',
    operations=[
        {'type': 'denoise', 'params': {'strength': 0.1}},
        {'type': 'crop', 'params': {'x': 100, 'y': 100, 'width': 500, 'height': 500}},
        {'type': 'normalize', 'params': {}}
    ]
)

# 文本预处理
processed_text = api.preprocess_text(
    text="患者男，56岁，主诉肝区疼痛3个月，既往有肝硬化病史...",
    operations=[
        {'type': 'clean', 'params': {}},
        {'type': 'standardize_terms', 'params': {}},
        {'type': 'extract_entities', 'params': {}}
    ]
)
```

### Flask集成API

数据集成模块提供了预定义的Flask蓝图，可以直接集成到现有的Flask应用中：

```python
from flask import Flask
from data_integration.flask_integration import data_integration_bp

app = Flask(__name__)

# 配置Flask应用
app.config.from_object('data_integration.config.get_flask_app_config(environment="development")')

# 注册数据集成蓝图
app.register_blueprint(data_integration_bp, url_prefix='/api/data')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
```

## 示例用例

### 示例1：单张病理图像上传与检索

```python
from data_integration import DataIntegrationAPI
import json

# 初始化API
api = DataIntegrationAPI(environment='development')

print("示例1：单张病理图像上传与检索")

# 1. 上传图像
print("上传病理图像...")
image_id = api.upload_image(
    image_path='sample_images/liver_cancer.tiff',
    metadata={
        'patient_id': 'P12345',
        'study_id': 'S54321',
        'modality': 'Histology',
        'acquisition_date': '2023-04-15',
        'description': '肝脏切片病理图像',
        'tissue_type': 'Liver',
        'stain_type': 'H&E',
        'magnification': '40x'
    },
    preprocess=True
)
print(f"图像上传成功，ID: {image_id}")

# 2. 根据患者ID检索
print("\n根据患者ID检索数据...")
patient_data = api.search_by_patient_id('P12345')
print(f"找到 {len(patient_data.get('images', []))} 张图像")

# 3. 查看图像元数据
for img in patient_data.get('images', []):
    print(f"图像ID: {img.get('id')}")
    print(f"描述: {img.get('metadata', {}).get('description')}")
    print(f"组织类型: {img.get('metadata', {}).get('tissue_type')}")
```

### 示例2：批量上传医学图像

```python
from data_integration import DataIntegrationAPI
import time

# 初始化API
api = DataIntegrationAPI(environment='development')

print("示例2：批量上传医学图像")

# 准备批量上传任务
upload_items = [
    {
        'image_path': 'sample_images/liver_cancer_1.tiff',
        'metadata': {
            'patient_id': 'P12345',
            'study_id': 'S54321',
            'modality': 'Histology',
            'description': '肝脏切片1'
        }
    },
    {
        'image_path': 'sample_images/liver_cancer_2.tiff',
        'metadata': {
            'patient_id': 'P12345',
            'study_id': 'S54321',
            'modality': 'Histology',
            'description': '肝脏切片2'
        }
    },
    {
        'image_path': 'sample_images/liver_cancer_3.tiff',
        'metadata': {
            'patient_id': 'P12345',
            'study_id': 'S54321',
            'modality': 'Histology',
            'description': '肝脏切片3'
        }
    }
]

# 创建批量上传任务
print("创建批量上传任务...")
batch_id = api.create_batch_upload(upload_items)
print(f"批量任务创建成功，ID: {batch_id}")

# 监控任务进度
print("\n监控任务进度:")
while True:
    status = api.get_batch_status(batch_id)
    print(f"状态: {status['status']}, 进度: {status['completed_count']}/{status['total_count']}")
    
    if status['status'] in ['completed', 'failed', 'cancelled']:
        break
    
    time.sleep(2)

if status['status'] == 'completed':
    print("\n所有图像上传完成!")
    for result in status.get('results', []):
        print(f"图像 {result.get('filename')} - 状态: {result.get('status')}, ID: {result.get('image_id')}")
else:
    print(f"\n任务状态: {status['status']}")
    if 'error' in status:
        print(f"错误信息: {status['error']}")
```

### 示例3：医疗系统数据整合

```python
from data_integration import DataIntegrationAPI

# 初始化API
api = DataIntegrationAPI(environment='development')

print("示例3：医疗系统数据整合")

# 患者ID
patient_id = 'P12345'

# 1. 获取患者基本信息
print(f"获取患者 {patient_id} 的基本信息...")
patient_info = api.get_patient_info(patient_id)
print(f"患者姓名: {patient_info.get('name')}")
print(f"年龄: {patient_info.get('age')}")
print(f"性别: {patient_info.get('gender')}")
print(f"诊断: {patient_info.get('diagnosis')}")

# 2. 获取患者病历记录
print(f"\n获取患者 {patient_id} 的病历记录...")
medical_records = api.get_medical_records(patient_id, start_date='2023-01-01')
print(f"找到 {len(medical_records)} 条病历记录")
for record in medical_records[:3]:  # 显示前3条
    print(f"- 日期: {record.get('date')}, 类型: {record.get('type')}, 摘要: {record.get('summary')[:100]}...")

# 3. 获取检验结果
print(f"\n获取患者 {patient_id} 的检验结果...")
lab_results = api.get_lab_results(patient_id)
print(f"找到 {len(lab_results)} 条检验结果")

# 4. 获取影像检查
print(f"\n获取患者 {patient_id} 的影像检查...")
imaging_studies = api.get_imaging_studies(patient_id)
print(f"找到 {len(imaging_studies)} 项影像检查")

# 5. 整合所有数据
print("\n整合患者数据...")
integrated_data = api.integrate_patient_data(patient_id)
print(f"\n整合数据完成:")
print(f"- 基本信息: 完整")
print(f"- 病历记录: {len(integrated_data.get('medical_records', []))} 条")
print(f"- 检验结果: {len(integrated_data.get('lab_results', []))} 条")
print(f"- 影像检查: {len(integrated_data.get('imaging_studies', []))} 项")
print(f"- 病理图像: {len(integrated_data.get('pathology_images', []))} 张")
```

## 扩展指南

### 添加新的上传处理器

要添加对新数据类型的支持，可以继承基础的上传处理器类并实现必要的方法：

```python
from data_integration.uploaders.base import BaseUploader

class CustomDataTypeUploader(BaseUploader):
    def __init__(self, config):
        super().__init__(config)
        # 初始化自定义上传器
    
    def validate(self, data_source):
        # 验证数据有效性
        pass
    
    def process(self, data_source, metadata=None):
        # 处理上传的数据
        pass
    
    def store(self, processed_data, metadata=None):
        # 存储处理后的数据
        pass
```

### 添加新的预处理操作

要扩展图像或文本处理器，添加新的预处理操作：

```python
from data_integration.preprocessors.image_preprocessor import ImageQualityEnhancer

# 扩展图像预处理器
class ExtendedImageEnhancer(ImageQualityEnhancer):
    def custom_enhancement(self, image, params):
        # 实现自定义增强操作
        enhanced_image = image  # 这里添加自定义处理逻辑
        return enhanced_image
    
    def process(self, image, operations):
        # 扩展处理方法以支持新操作
        for op in operations:
            if op['type'] == 'custom_enhancement':
                image = self.custom_enhancement(image, op.get('params', {}))
            else:
                # 调用父类方法处理标准操作
                image = super().process_single_operation(image, op)
        return image
```

## 故障排除

### 常见问题与解决方案

#### 1. 图像上传失败

**问题**：无法上传大尺寸的病理图像
**解决方案**：
- 检查配置文件中的 `max_file_size_mb` 和 `max_total_size_mb` 设置
- 确保服务器有足够的磁盘空间
- 检查文件格式是否在 `allowed_formats` 列表中

#### 2. 医疗系统连接错误

**问题**：无法连接到HIS/EMR/PACS系统
**解决方案**：
- 验证系统URL和认证信息是否正确
- 检查网络连接和防火墙设置
- 确认医疗系统API是否可用
- 查看日志文件中的详细错误信息

#### 3. 检索性能问题

**问题**：数据检索速度慢
**解决方案**：
- 调整 `retrieval_engine` 配置中的 `default_limit` 参数
- 确保启用了缓存功能 (`enable_caching: true`)
- 优化查询条件，减少返回的数据量

#### 4. Flask集成问题

**问题**：API接口返回错误
**解决方案**：
- 检查Flask配置是否正确
- 确认数据集成模块已正确注册
- 查看Flask日志获取详细错误信息
- 验证请求参数格式是否符合API要求

## 贡献指南

欢迎贡献代码！请按照以下步骤进行：

1. Fork项目仓库
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启Pull Request

## 许可证

[MIT License](LICENSE)

## 联系方式

如有任何问题或建议，请联系项目维护团队。

## 更新日志

### v1.0.0 (初始版本)
- 支持多渠道医学数据上传
- 实现数据预处理功能
- 提供统一存储和检索接口
- 支持Flask应用集成
- 包含医疗系统对接功能