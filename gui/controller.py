import asyncio
import queue
import threading
import logging
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import urllib.parse # 需要导入 urllib.parse

# 假设 data_module 在父目录中
# from ..data_module import TaskFactory, base_task  # Relative import fails when run with -m
# 使用绝对导入，假设项目根目录在 sys.path 中
from data_module import TaskFactory, base_task

# --- 配置 ---
# 确定相对于此文件的路径
CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data_module', 'config.json')

# --- 用于线程通信的队列 ---
request_queue = queue.Queue()  # GUI -> 后端线程
response_queue = queue.Queue() # 后端线程 -> GUI

# --- 内部状态 ---
_task_list_cache: List[Dict[str, Any]] = [] # 任务详情和选择状态的缓存
_running_task_status: Dict[str, Dict[str, Any]] = {} # 当前运行中任务的状态
_stop_requested = False # 用于发出停止任务信号的标志 (基础版本)
_backend_thread: Optional[threading.Thread] = None # 跟踪线程
_backend_running = False # 指示异步循环是否活动的标志

# --- 日志设置 ---
class QueueHandler(logging.Handler):
    """Send log records to the response queue."""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue.put(('LOG_ENTRY', log_entry))

# --- 后端异步循环 ---
def _start_async_loop():
    """Target function for the background thread."""
    global _backend_running
    _backend_running = True
    logging.info("后台异步循环启动")
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_process_requests())
    except Exception as e:
        logging.exception("后台异步循环异常终止")
        response_queue.put(('ERROR', f"后台严重错误: {e}"))
    finally:
        loop.close()
        _backend_running = False
        logging.info("后台异步循环已关闭。")

async def _process_requests():
    """The main async function processing requests from the GUI."""
    global _stop_requested, _backend_running
    
    # 配置日志处理器
    log_queue = response_queue
    # 配置日志处理器
    queue_handler = QueueHandler(log_queue)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S')
    queue_handler.setFormatter(formatter)
    root_logger = logging.getLogger() # 同时捕获来自 data_module 的日志
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(logging.INFO) # 设置所需的日志级别

    # 初始化 TaskFactory
    try:
        await TaskFactory.initialize()
        response_queue.put(('STATUS', '后台初始化完成'))
    except Exception as e:
        response_queue.put(('ERROR', f"TaskFactory 初始化失败: {e}"))
        return

    while _backend_running: # 检查标志而不是 True
        try:
            # 使用带有短超时的非阻塞 get 以允许检查 _backend_running
            request_type, data = request_queue.get(timeout=0.1)
        except queue.Empty:
            await asyncio.sleep(0.05) # 防止忙等待
            continue # 再次检查 _backend_running 标志

        try:
            if request_type == 'GET_TASKS':
                await _handle_get_tasks()
            elif request_type == 'TOGGLE_SELECT':
                _handle_toggle_select(data) # data = 行索引
            elif request_type == 'EXECUTE_TASKS':
                # 开始前重置停止标志
                _stop_requested = False
                # 在后台执行，不阻塞队列处理
                asyncio.create_task(_handle_execute_tasks(data['mode'], data['start_date']))
            elif request_type == 'REQUEST_STOP':
                 _stop_requested = True
                 response_queue.put(('LOG_ENTRY', "收到停止请求，将尝试在下一个任务开始前停止..."))
            elif request_type == 'SHUTDOWN':
                logging.info("收到关闭请求，开始关闭...")
                await TaskFactory.shutdown()
                response_queue.put(('LOG_ENTRY', "后台服务已正常关闭。"))
                _backend_running = False # 设置标志以退出循环
                # 此处不需要 break，循环条件将处理退出
            elif request_type == 'SELECT_SPECIFIC':
                _handle_select_specific(data) # data = 任务名称列表
            elif request_type == 'DESELECT_SPECIFIC':
                _handle_deselect_specific(data) # data = 任务名称列表
            elif request_type == 'SAVE_SETTINGS':
                await _handle_save_settings(data) # data = 来自 GUI 的设置
            else:
                 response_queue.put(('LOG_ENTRY', f"未知请求类型: {request_type}"))

        except Exception as e:
            logging.exception(f"处理请求 {request_type} 时出错") # 记录异常详情
            response_queue.put(('ERROR', f"处理请求 {request_type} 时出错: {e}"))

# --- 请求处理器 (在异步循环中运行) ---
async def _handle_get_tasks():
    """Fetch task list from factory, update cache, and send formatted list to GUI."""
    global _task_list_cache
    try:
        # TaskFactory.get_all_task_names() 可能是同步的
        task_names = TaskFactory.get_all_task_names() # 移除了 await
        new_cache = []
        existing_selection = {item['name']: item['selected'] for item in _task_list_cache} # 保留选择状态

        for name in sorted(task_names): # 首先按字母顺序排序
             try:
                 task_instance = await TaskFactory.get_task(name)
                 selected = existing_selection.get(name, False) # 保留选择状态

                 # --- 增强的类型提取 ---
                 parts = name.split('_')
                 task_type = 'unknown' # 默认类型
                 if len(parts) > 1:
                     # 映射特定前缀或使用第二部分
                     prefix = parts[0]
                     second_part = parts[1]
                     if prefix == 'tushare':
                         if second_part == 'fina':
                             task_type = 'finance'
                         elif second_part in ['stock', 'fund', 'index']:
                             task_type = second_part
                         else:
                             # 如果需要，为其他 tushare 类型提供回退
                             task_type = second_part
                     else:
                         # 对于非 tushare 任务，也许使用前缀？
                         task_type = prefix
                 # --- 增强的类型提取结束 ---

                 new_cache.append({
                     'name': name,
                     # 'type': name.split('_')[0] if '_' in name else 'unknown', # Old type extraction
                     'type': task_type, # 使用增强类型
                     'description': getattr(task_instance, 'description', ''),
                     'selected': selected
                 })
             except Exception as e:
                  logging.error(f"获取任务 {name} 详情失败: {e}")
                  # 添加错误状态？或跳过？暂时跳过。

        # 按类型然后按名称排序以供显示
        _task_list_cache = sorted(new_cache, key=lambda x: (x['type'], x['name']))

        # 发送格式化数据给 Tkinter Treeview
        response_queue.put(('TASK_LIST_UPDATE', _format_task_list_for_tkinter_treeview()))
        response_queue.put(('STATUS', '任务列表已刷新')) # 添加状态更新
    except Exception as e:
        logging.exception("获取任务列表失败")
        response_queue.put(('ERROR', f"获取任务列表失败: {e}"))

def _format_task_list_for_tkinter_treeview() -> List[Dict[str, Any]]:
    """Format the cache for Tkinter Treeview (returns list of dicts)."""
    # 返回所需信息，GUI 端将格式化 'selected' 列
    return [
        {
            'selected': task['selected'], # 直接传递布尔值
            'type': task['type'],
            'name': task['name'],
            'description': task['description']
         }
        for task in _task_list_cache
    ]

def _handle_toggle_select(row_index: int):
    """Toggle selection state for a task by index and send update."""
    if 0 <= row_index < len(_task_list_cache):
        _task_list_cache[row_index]['selected'] = not _task_list_cache[row_index]['selected']
        # 将更新后的格式化列表发送回 GUI
        response_queue.put(('TASK_LIST_UPDATE', _format_task_list_for_tkinter_treeview()))

def _handle_select_specific(task_names: List[str]):
    """Set selection state to True for specific tasks and send update."""
    print(f"Controller: Handling SELECT_SPECIFIC for {len(task_names)} tasks.")
    changed = False
    task_name_set = set(task_names) # 使用集合以加快查找速度
    for task in _task_list_cache:
        if task['name'] in task_name_set:
            if not task['selected']: # 仅当状态实际翻转时才标记为已更改
                task['selected'] = True
                changed = True
    if changed:
        response_queue.put(('TASK_LIST_UPDATE', _format_task_list_for_tkinter_treeview()))
    else:
        print("Controller: No state changed during SELECT_SPECIFIC.")

def _handle_deselect_specific(task_names: List[str]):
    """Set selection state to False for specific tasks and send update."""
    print(f"Controller: Handling DESELECT_SPECIFIC for {len(task_names)} tasks.")
    changed = False
    task_name_set = set(task_names) # 使用集合以加快查找速度
    for task in _task_list_cache:
        if task['name'] in task_name_set:
            if task['selected']: # 仅当状态实际翻转时才标记为已更改
                task['selected'] = False
                changed = True
    if changed:
        response_queue.put(('TASK_LIST_UPDATE', _format_task_list_for_tkinter_treeview()))
    else:
        print("Controller: No state changed during DESELECT_SPECIFIC.")

async def _handle_execute_tasks(mode: str, start_date_str: Optional[str]):
    """Handle the request to execute selected tasks."""
    global _running_task_status, _stop_requested
    
    selected_tasks = [task for task in _task_list_cache if task['selected']]
    if not selected_tasks:
        response_queue.put(('LOG_ENTRY', "没有选择任何任务。"))
        response_queue.put(('RUN_COMPLETED', "没有任务运行"))
        return

    _running_task_status = {
        task['name']: {'type': task['type'], 'name': task['name'], 'status': '排队中', 'progress': '0%', 'start': '', 'end': ''}
        for task in selected_tasks
    }
    # 发送初始状态以更新运行表
    response_queue.put(('RUN_TABLE_INIT', list(_running_task_status.values())))
    response_queue.put(('STATUS', '开始执行任务批次...'))

    tasks_executed_count = 0
    tasks_succeeded_count = 0
    tasks_failed_count = 0

    for task_info in selected_tasks:
        if _stop_requested:
            response_queue.put(('LOG_ENTRY', "任务执行已手动停止。"))
            _running_task_status[task_info['name']]['status'] = '已跳过'
            response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_info['name']]))
            continue # 跳过剩余任务

        task_name = task_info['name']
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _running_task_status[task_name].update({'status': '运行中', 'progress': '0%', 'start': start_time, 'end': ''})
        response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_name]))
        
        try:
            task: base_task.Task = await TaskFactory.get_task(task_name)
            
            # 根据模式准备参数
            kwargs = {}
            execute_method = None

            if mode == "全量导入":
                 # 如果任务支持 'full' 模式，则使用一个非常早的日期或进行特殊处理
                 kwargs['start_date'] = '19900101' # 示例最早日期
                 kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                 execute_method = task.execute
            elif mode == "智能增量":
                 # 假设 smart_incremental_update 存在并且不从 GUI 获取额外的日期参数
                 execute_method = task.smart_incremental_update
                 # smart_incremental_update 可能需要 **kwargs，暂时传递空字典
            elif mode == "手动增量":
                 if not start_date_str:
                     raise ValueError("手动增量模式需要指定开始日期")
                 kwargs['start_date'] = start_date_str.replace('-', '') # 确保格式为 YYYYMMDD
                 kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                 execute_method = task.execute
            else:
                raise ValueError(f"未知的执行模式: {mode}")

            if not execute_method:
                 raise NotImplementedError(f"任务 {task_name} 不支持模式 {mode}")

            # 执行任务方法
            result = await execute_method(**kwargs)
            
            tasks_executed_count += 1
            status = result.get('status', 'unknown') if isinstance(result, dict) else 'unknown_result'
            
            if status in ['success', 'partial_success', 'up_to_date', 'no_data']:
                tasks_succeeded_count += 1
                final_status = '完成' if status != 'partial_success' else '部分完成'
                _running_task_status[task_name]['status'] = final_status
                _running_task_status[task_name]['progress'] = '100%'
            else:
                tasks_failed_count += 1
                _running_task_status[task_name]['status'] = f"失败 ({status})"
                _running_task_status[task_name]['progress'] = '-' # 指示失败
                if isinstance(result, dict) and 'error' in result:
                     response_queue.put(('LOG_ENTRY', f"任务 {task_name} 失败: {result['error']}"))
            
        except Exception as e:
            tasks_executed_count += 1
            tasks_failed_count += 1
            logging.exception(f"执行任务 {task_name} 时发生严重错误")
            _running_task_status[task_name]['status'] = '严重错误'
            _running_task_status[task_name]['progress'] = '-'
            response_queue.put(('LOG_ENTRY', f"执行任务 {task_name} 时发生严重错误: {e}"))
        finally:
            _running_task_status[task_name]['end'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_name]))
            await asyncio.sleep(0.1) # 任务之间的小延迟

    final_summary = f"批次执行完成: 共执行{tasks_executed_count}个任务, 成功{tasks_succeeded_count}, 失败{tasks_failed_count}."
    response_queue.put(('LOG_ENTRY', final_summary))
    response_queue.put(('RUN_COMPLETED', final_summary))
    _stop_requested = False # 批处理完成后重置停止标志

# --- 设置处理 ---
def _load_settings() -> Dict:
    """加载配置文件 (config.json)"""
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            settings = json.load(f)
            logging.info(f"从 {CONFIG_FILE_PATH} 加载设置成功。")
            return settings
    except FileNotFoundError:
        logging.warning(f"配置文件 {CONFIG_FILE_PATH} 未找到，将使用空设置。")
        return {}
    except json.JSONDecodeError:
        logging.error(f"解析配置文件 {CONFIG_FILE_PATH} 失败。")
        return {}
    except Exception as e:
        logging.exception(f"加载配置文件时发生未知错误")
        return {}

def _save_settings(settings: Dict) -> bool:
    """保存设置到配置文件 (config.json)"""
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH), exist_ok=True)
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4, ensure_ascii=False)
        logging.info(f"设置已成功保存到 {CONFIG_FILE_PATH}")
        response_queue.put(('LOG_ENTRY', '配置设置已成功保存。')) # 反馈给 GUI
        # 可以考虑重新初始化 TaskFactory 或通知它配置已更改
        # asyncio.create_task(TaskFactory.reload_config(settings)) # 如果需要动态重载
        return True
    except IOError as e:
        logging.error(f"写入配置文件 {CONFIG_FILE_PATH} 时出错: {e}")
        response_queue.put(('ERROR', f"保存配置失败: 文件写入错误 ({e})。"))
        return False
    except Exception as e:
        logging.exception(f"保存配置文件时发生未知错误")
        response_queue.put(('ERROR', f"保存配置失败: 未知错误 ({e})。"))
        return False

# --- GUI 公共接口 ---

def initialize_controller():
    """启动后台处理线程。"""
    global _backend_thread
    if _backend_thread is None or not _backend_thread.is_alive():
        _backend_thread = threading.Thread(target=_start_async_loop, daemon=True)
        _backend_thread.start()
        logging.info("后台处理线程已启动。")
    else:
        logging.warning("尝试初始化控制器，但后台线程已在运行。")

def request_task_list():
    """向后台请求任务列表。"""
    request_queue.put(('GET_TASKS', None))

def toggle_task_selection(row_index: int):
    """请求切换指定索引任务的选择状态。"""
    request_queue.put(('TOGGLE_SELECT', row_index))

def request_select_specific(task_names: List[str]):
    """请求将指定列表中的任务设置为选中状态。"""
    request_queue.put(('SELECT_SPECIFIC', task_names))

def request_deselect_specific(task_names: List[str]):
    """请求将指定列表中的任务设置为未选中状态。"""
    request_queue.put(('DESELECT_SPECIFIC', task_names))

# --- 新增方法 --- #
def get_cached_task_list() -> List[Dict[str, Any]]:
    """返回当前缓存的任务列表，供 GUI 处理点击事件时查找索引。"""
    # 返回缓存的副本以避免外部修改
    return list(_task_list_cache)

def get_current_settings() -> Dict:
    """加载完整配置，提取 GUI 需要的部分返回。"""
    full_config = _load_settings()
    # 提取 token
    tushare_token = full_config.get('api', {}).get('tushare_token', '')
    # 提取 database 字典 (可能为空)
    database_config = full_config.get('database', {})
    # 返回给 GUI 处理
    return {
        'tushare_token': tushare_token,
        'database': database_config # 包含 url 或其他可能的 db 设置
    }

def save_settings(settings_from_gui: Dict):
    """(接口) 请求后台异步保存给定的设置字典。"""
    print(f"Controller (GUI): Received save request for: {settings_from_gui}")
    request_queue.put(('SAVE_SETTINGS', settings_from_gui))

def request_task_execution(mode: str, start_date: Optional[str]):
    """向后台请求执行任务。"""
    request_queue.put(('EXECUTE_TASKS', {'mode': mode, 'start_date': start_date}))

def request_stop_execution():
    """向后台请求停止当前任务批次的执行。"""
    request_queue.put(('REQUEST_STOP', None))

def request_shutdown():
    """向后台请求关闭服务。"""
    request_queue.put(('SHUTDOWN', None))

def is_backend_running() -> bool:
    """检查后台异步循环是否仍在运行。"""
    return _backend_running

def check_for_updates() -> List:
    """从响应队列中获取所有待处理的更新。"""
    updates = []
    while not response_queue.empty():
        try:
            update = response_queue.get_nowait()
            updates.append(update)
        except queue.Empty:
            break
    return updates

# 重命名内部保存函数
def _perform_save_settings(settings_from_gui: Dict) -> bool:
    """实际执行保存设置到文件系统的操作 (在后台线程调用)。"""
    print(f"Controller (BG): Performing save with data: {settings_from_gui}")
    new_token = settings_from_gui.get('tushare_token')
    new_db_url = settings_from_gui.get('database_url')

    if new_token is None:
        response_queue.put(('ERROR', '保存设置失败：未提供 Tushare Token。'))
        return False

    try:
        full_config = _load_settings()
        if not full_config:
            full_config = {'database': {}, 'api': {}, 'tasks': {}}
            print("Controller (BG): Creating new config structure.")

        if 'api' not in full_config: full_config['api'] = {}
        if 'database' not in full_config: full_config['database'] = {}

        # 直接使用从 GUI 接收的结构来更新
        full_config['api']['tushare_token'] = new_token
        full_config['database']['url'] = new_db_url
        print(f"Controller (BG): Updated config: api.token={new_token}, db.url={new_db_url}")

        # 调用原始的文件保存函数
        saved_file = _save_settings(full_config) # _save_settings 现在只负责写文件和发送LOG
        print(f"Controller (BG): File save result: {saved_file}")
        return saved_file
    except Exception as e:
        logging.exception("Controller (BG): _perform_save_settings Exception")
        response_queue.put(('ERROR', f"保存设置时发生内部错误: {e}"))
        return False

# 添加处理保存和重载的异步 Handler
async def _handle_save_settings(settings_from_gui: Dict):
    """处理 SAVE_SETTINGS 请求：保存配置，如果成功则重载 TaskFactory。"""
    saved_ok = _perform_save_settings(settings_from_gui)

    if saved_ok:
        response_queue.put(('STATUS', '设置已成功保存，正在尝试重新加载配置...'))
        try:
            # !! 重要 !!: TaskFactory 需要有 reload_config 方法
            print("Controller (BG): Calling TaskFactory.reload_config()...")
            await TaskFactory.reload_config() # 假设是异步类方法或通过实例调用
            print("Controller (BG): TaskFactory.reload_config() completed.")
            response_queue.put(('LOG_ENTRY', '后台任务配置已根据新设置重新加载。'))
            response_queue.put(('STATUS', '设置已保存并重新加载。'))
        except AttributeError:
            logging.error("TaskFactory 没有 reload_config 方法！无法动态重载配置。")
            response_queue.put(('ERROR', '保存成功，但无法自动重载任务配置 (缺少方法)。请重启应用生效。'))
            response_queue.put(('STATUS', '设置已保存，请重启应用生效。'))
        except Exception as e:
            logging.exception("调用 TaskFactory.reload_config() 时出错")
            response_queue.put(('ERROR', f"保存成功，但重载任务配置时出错: {e}"))
            response_queue.put(('STATUS', '设置已保存但重载失败。'))
    # else: # 保存失败，错误消息已由 _perform_save_settings 发送
    #     response_queue.put(('STATUS', '设置保存失败。'))
