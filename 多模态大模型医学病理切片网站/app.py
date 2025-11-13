from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os
from werkzeug.utils import secure_filename

# 初始化Flask应用
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'  # 生产环境中应使用安全的密钥
app.config['UPLOAD_FOLDER'] = 'uploads/'  # 上传文件的保存路径
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 最大上传文件大小16MB

# 确保上传文件夹存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'tiff', 'bmp', 'svs'}

def allowed_file(filename):
    """检查文件扩展名是否被允许"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# 首页路由
@app.route('/')
def index():
    """显示首页"""
    return render_template('index.html')

# 上传病理切片路由
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    """处理病理切片上传"""
    if request.method == 'POST':
        # 检查请求是否包含文件部分
        if 'file' not in request.files:
            flash('没有文件部分')
            return redirect(request.url)
        
        file = request.files['file']
        # 如果用户没有选择文件，浏览器也会提交一个空部分
        if file.filename == '':
            flash('没有选择文件')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            flash('文件上传成功')
            return redirect(url_for('view_slide', filename=filename))
        else:
            flash('不支持的文件类型')
            return redirect(request.url)
    
    return render_template('upload.html')

# 查看病理切片路由
@app.route('/view/<filename>')
def view_slide(filename):
    """显示病理切片"""
    return render_template('view_slide.html', filename=filename)

# API路由 - 获取切片信息
@app.route('/api/slide/<filename>')
def get_slide_info(filename):
    """获取切片的元数据信息"""
    # 这里可以添加实际的切片处理逻辑
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': '文件不存在'}), 404
    
    # 模拟获取切片信息
    slide_info = {
        'filename': filename,
        'size': os.path.getsize(filepath),
        'path': filepath
    }
    
    return jsonify(slide_info)

# 错误处理
@app.errorhandler(404)
def page_not_found(e):
    """404错误处理"""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    """500错误处理"""
    return render_template('500.html'), 500

# 主函数
if __name__ == '__main__':
    app.run(debug=True)