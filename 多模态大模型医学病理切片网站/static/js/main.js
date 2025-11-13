// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化工具提示
    initTooltips();
    
    // 添加页面淡入效果
    addPageFadeIn();
    
    // 初始化表单验证
    initFormValidation();
    
    // 监听滚动事件
    initScrollEffects();
    
    // 添加平滑滚动
    initSmoothScroll();
});

/**
 * 初始化Bootstrap工具提示
 */
function initTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    const tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    });
}

/**
 * 添加页面淡入效果
 */
function addPageFadeIn() {
    document.body.classList.add('fade-in');
}

/**
 * 初始化表单验证
 */
function initFormValidation() {
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', function(event) {
            // 这里可以添加自定义验证逻辑
            if (!this.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
                // 显示验证错误提示
                const invalidInputs = this.querySelectorAll(':invalid');
                if (invalidInputs.length > 0) {
                    invalidInputs[0].focus();
                }
            }
            // 添加was-validated类以显示Bootstrap的验证样式
            this.classList.add('was-validated');
        });
    });
}

/**
 * 初始化滚动效果
 */
function initScrollEffects() {
    window.addEventListener('scroll', function() {
        // 导航栏滚动效果
        const navbar = document.querySelector('.navbar');
        if (navbar) {
            if (window.scrollY > 50) {
                navbar.classList.add('shadow-md');
                navbar.classList.remove('shadow-sm');
            } else {
                navbar.classList.remove('shadow-md');
                navbar.classList.add('shadow-sm');
            }
        }
        
        // 显示/隐藏回到顶部按钮
        const backToTopBtn = document.getElementById('back-to-top');
        if (backToTopBtn) {
            if (window.scrollY > 300) {
                backToTopBtn.classList.remove('d-none');
            } else {
                backToTopBtn.classList.add('d-none');
            }
        }
    });
}

/**
 * 初始化平滑滚动
 */
function initSmoothScroll() {
    // 为所有锚点链接添加平滑滚动
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            
            const targetId = this.getAttribute('href');
            if (targetId === '#') return;
            
            const targetElement = document.querySelector(targetId);
            if (targetElement) {
                window.scrollTo({
                    top: targetElement.offsetTop - 80, // 考虑导航栏高度
                    behavior: 'smooth'
                });
            }
        });
    });
    
    // 回到顶部按钮
    const backToTopBtn = document.getElementById('back-to-top');
    if (!backToTopBtn) {
        // 如果不存在，创建一个回到顶部按钮
        const btn = document.createElement('button');
        btn.id = 'back-to-top';
        btn.className = 'btn btn-primary fixed-bottom m-4 rounded-circle p-3 shadow-lg d-none';
        btn.setAttribute('title', '回到顶部');
        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-up" viewBox="0 0 16 16">
            <path fill-rule="evenodd" d="M8 15a.5.5 0 0 0 .5-.5V2.707l3.146 3.147a.5.5 0 0 0 .708-.708l-4-4a.5.5 0 0 0-.708 0l-4 4a.5.5 0 1 0 .708.708L7.5 2.707V14.5a.5.5 0 0 0 .5.5z"/>
        </svg>`;
        document.body.appendChild(btn);
        
        btn.addEventListener('click', function() {
            window.scrollTo({
                top: 0,
                behavior: 'smooth'
            });
        });
    } else {
        backToTopBtn.addEventListener('click', function() {
            window.scrollTo({
                top: 0,
                behavior: 'smooth'
            });
        });
    }
}

/**
 * 显示通知消息
 * @param {string} message - 消息内容
 * @param {string} type - 消息类型 (success, error, warning, info)
 * @param {number} duration - 显示时长(毫秒)
 */
function showNotification(message, type = 'info', duration = 3000) {
    // 检查是否已存在通知容器
    let notificationContainer = document.getElementById('notification-container');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.id = 'notification-container';
        notificationContainer.className = 'fixed top-0 end-0 p-4 z-50';
        document.body.appendChild(notificationContainer);
    }
    
    // 创建通知元素
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show shadow-lg`;
    notification.role = 'alert';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // 添加到容器
    notificationContainer.appendChild(notification);
    
    // 自动关闭
    setTimeout(() => {
        const bsAlert = bootstrap.Alert.getInstance(notification);
        if (bsAlert) {
            bsAlert.close();
        } else {
            notification.remove();
        }
    }, duration);
}

/**
 * 格式化文件大小
 * @param {number} bytes - 字节数
 * @returns {string} 格式化后的文件大小
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * 验证文件类型
 * @param {File} file - 文件对象
 * @param {Array} allowedTypes - 允许的MIME类型数组
 * @returns {boolean} 是否为允许的文件类型
 */
function isValidFileType(file, allowedTypes) {
    return allowedTypes.includes(file.type);
}

/**
 * 加载中指示器
 */
const LoadingIndicator = {
    show: function() {
        let loadingElement = document.getElementById('global-loading');
        if (!loadingElement) {
            loadingElement = document.createElement('div');
            loadingElement.id = 'global-loading';
            loadingElement.className = 'fixed inset-0 bg-dark bg-opacity-50 d-flex align-items-center justify-content-center z-50';
            loadingElement.innerHTML = `
                <div class="bg-white p-5 rounded-lg shadow-lg">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">加载中...</span>
                    </div>
                    <p class="mt-3">正在处理，请稍候...</p>
                </div>
            `;
            document.body.appendChild(loadingElement);
        }
        loadingElement.style.display = 'flex';
    },
    hide: function() {
        const loadingElement = document.getElementById('global-loading');
        if (loadingElement) {
            loadingElement.style.display = 'none';
        }
    }
};

// 导出公共方法
window.showNotification = showNotification;
window.formatFileSize = formatFileSize;
window.isValidFileType = isValidFileType;
window.LoadingIndicator = LoadingIndicator;