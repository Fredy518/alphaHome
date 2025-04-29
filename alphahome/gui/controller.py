import asyncio
import queue
import threading
import logging
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Callable
import urllib.parse # 需要导入 urllib.parse
import appdirs # <-- 导入 appdirs
import traceback

# 使用绝对导入，假设项目根目录在 sys.path 中
from ..data_module import TaskFactory, base_task

# --- 配置 ---
# 使用 appdirs 获取用户配置目录
APP_NAME = "alphaHomeApp" # <--- 您可以修改应用名称
APP_AUTHOR = "YourAppNameOrAuthor" # <--- 建议修改为您的名称或组织名
CONFIG_DIR = appdirs.user_config_dir(APP_NAME, APP_AUTHOR)
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR, 'config.json') # 配置文件路径现在指向用户目录

# --- 用于线程通信的队列 ---
request_queue = queue.Queue()  # GUI -> 后端线程
response_queue = queue.Queue() # 后端线程 -> GUI

# --- 内部状态 ---
_task_list_cache: List[Dict[str, Any]] = [] # 任务详情和选择状态的缓存
_running_task_status: Dict[str, Dict[str, Any]] = {} # 当前运行中任务的状态
_stop_requested = False # 用于发出停止任务信号的标志 (基础版本)
_backend_thread: Optional[threading.Thread] = None # 跟踪线程
_backend_running = False # 指示异步循环是否活动的标志
_current_stop_event: Optional[asyncio.Event] = None

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
    root_logger.setLevel(logging.INFO) # 恢复为 INFO 级别

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
                 if _current_stop_event:
                      _current_stop_event.set()
                      response_queue.put(('LOG_ENTRY', "收到停止请求，信号已发送给当前任务..."))
                 else:
                      response_queue.put(('LOG_ENTRY', "收到停止请求，但当前没有任务在运行或任务不支持停止。"))
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
        # 尝试获取任务名称，这可能会因为 TaskFactory 未初始化而失败
        task_names = TaskFactory.get_all_task_names() # 可能引发 RuntimeError
        
        # --- 如果成功获取 task_names，继续正常处理 ---
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
                     'type': task_type,
                     'description': getattr(task_instance, 'description', ''),
                     'selected': selected,
                     'table_name': getattr(task_instance, 'table_name', None) # 添加 table_name
                 })
             except Exception as e:
                  logging.error(f"获取任务 {name} 详情失败: {e}")
                  # 添加错误状态？或跳过？暂时跳过。

        # 按类型然后按名称排序以供显示
        _task_list_cache = sorted(new_cache, key=lambda x: (x['type'], x['name']))

        # 获取 DBManager 实例
        db_manager = TaskFactory.get_db_manager()
        if not db_manager:
            logging.warning("DBManager not available, cannot fetch update times.")
            for task_detail in _task_list_cache:
                 task_detail['latest_update_time'] = "N/A (DB Error)"
            response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))
            return

        # --- 新的并发查询方式 --- 
        query_coroutines = []
        tasks_to_query_info = [] # List of (index, table_name) tuples

        # 1. 收集需要查询的任务和协程
        for index, task_detail in enumerate(_task_list_cache):
            table_name = task_detail.get('table_name')
            if table_name:
                # Request the raw datetime object for GUI display
                coro = db_manager.get_latest_date(table_name, 'update_time', return_raw_object=True)
                query_coroutines.append(coro)
                tasks_to_query_info.append((index, table_name))
            else:
                # 对于没有 table_name 的任务，直接设置默认值
                task_detail['latest_update_time'] = "N/A (No Table)"

        # 2. 并发执行查询
        if query_coroutines:
            logging.info(f"Starting concurrent query for {len(query_coroutines)} table timestamps...")
            results = await asyncio.gather(*query_coroutines, return_exceptions=True)
            logging.info("Concurrent timestamp query finished.")

            # 3. 处理查询结果
            for i, result in enumerate(results):
                task_index, table_name = tasks_to_query_info[i]
                latest_timestamp_str = "N/A" # 重置默认值

                if isinstance(result, Exception):
                    logging.warning(f"Error querying latest timestamp for table {table_name}: {type(result).__name__} - {result}")
                    latest_timestamp_str = "N/A (Query Error)"
                else:
                    # result 是查询结果 (时间戳) 或 None
                    latest_timestamp = result 
                    # --- 日志记录结束 --- 
                        
                    if latest_timestamp is not None:
                        # 预期 latest_timestamp 是 datetime 对象或 None
                        if isinstance(latest_timestamp, datetime):
                            # 直接格式化为 YYYY-MM-DD HH:MM:SS
                            latest_timestamp_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            # 其他类型，尝试转为字符串
                            logging.warning(f"Expected datetime object for {table_name}, but got {type(latest_timestamp)}. Converting to string.")
                            latest_timestamp_str = str(latest_timestamp)
                    else:
                        latest_timestamp_str = "No Data"

                # 更新缓存中的对应任务
                if 0 <= task_index < len(_task_list_cache):
                    _task_list_cache[task_index]['latest_update_time'] = latest_timestamp_str
                else:
                    logging.error(f"Task index {task_index} out of bounds while processing timestamp results.")
        # --- 并发查询结束 --- 

        logging.info(f"Sending updated TASK_LIST with {len(_task_list_cache)} tasks to GUI.")
        response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))
        response_queue.put(('STATUS', '任务列表已刷新')) # 添加状态更新
    except RuntimeError as e:
        # --- 专门处理 TaskFactory 未初始化的 RuntimeError ---
        if "TaskFactory 尚未初始化" in str(e):
            logging.warning("获取任务列表失败，因为 TaskFactory 尚未初始化。请用户配置数据库。")
            # 发送状态消息，而不是错误弹窗
            response_queue.put(('STATUS', "数据库未配置，无法加载任务。请前往'存储设置'配置并保存。"))
            # 清空缓存并更新 GUI 列表为空
            _task_list_cache = []
            response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))
        else:
            # 其他类型的 RuntimeError，仍然作为错误处理
            logging.exception("获取任务列表时发生意外的 RuntimeError")
            response_queue.put(('ERROR', f"获取任务列表时发生运行时错误: {e}"))
            
    except Exception as e:
        # --- 处理其他所有异常 --- 
        logging.exception("获取任务列表失败")
        # 仍然作为错误发送给 GUI
        response_queue.put(('ERROR', f"获取任务列表失败: {e}"))

def _format_task_list_for_tkinter_treeview() -> List[Dict[str, Any]]:
    """Format the cache for Tkinter Treeview (returns list of dicts)."""
    # 返回所需信息，GUI 端将格式化 'selected' 列
    logging.warning("_format_task_list_for_tkinter_treeview is likely obsolete as full cache is sent.")
    return _task_list_cache # 暂时返回完整缓存

def _handle_toggle_select(row_index: int):
    """Toggle selection state for a task by index and send update."""
    if 0 <= row_index < len(_task_list_cache):
        _task_list_cache[row_index]['selected'] = not _task_list_cache[row_index]['selected']
        # 将更新后的完整缓存列表发送回 GUI
        response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))

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
        response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))
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
        response_queue.put(('TASK_LIST_UPDATE', _task_list_cache))
    else:
        print("Controller: No state changed during DESELECT_SPECIFIC.")

async def _handle_execute_tasks(mode: str, start_date_str: Optional[str]):
    """Handle the request to execute selected tasks."""
    global _running_task_status, _current_stop_event
    
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

    # <<< Define the progress callback >>>
    async def _update_gui_progress(task_name: str, progress_str: str):
        """Callback function to update GUI progress from the task."""
        if task_name in _running_task_status:
            # Only update if the status is still '运行中' to avoid overwriting final status
            if _running_task_status[task_name]['status'] == '运行中':
                 _running_task_status[task_name]['progress'] = progress_str
                 response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_name]))
        else:
            # Log if task is not found, might indicate a race condition or error
            logging.warning(f"Received progress update for unknown or completed task: {task_name}")

    tasks_executed_count = 0
    tasks_succeeded_count = 0
    tasks_failed_count = 0

    stop_event = asyncio.Event()
    _current_stop_event = stop_event

    for task_info in selected_tasks:
        if stop_event.is_set():
            logging.info(f"任务 '{task_info['name']}' 因收到停止请求而被跳过 (循环中断)")
            _running_task_status[task_info['name']] = {'name': task_info['name'], 'status': '已停止', 'start_time': datetime.now().strftime("%H:%M:%S"), 'end_time': '-', 'progress': '-', 'details': '-'}
            response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_info['name']]))
            break

        task_name = task_info['name']
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _running_task_status[task_name].update({'status': '运行中', 'progress': '0%', 'start': start_time, 'end': ''})
        response_queue.put(('RUN_STATUS_UPDATE', _running_task_status[task_name]))
        
        try:
            task: base_task.Task = await TaskFactory.get_task(task_name)
            
            # 根据模式准备参数
            kwargs = {
                'start_date': start_date_str,
                'end_date': datetime.now().strftime('%Y%m%d'),
                'progress_callback': _update_gui_progress,
                'concurrent_limit': task_settings.get('concurrent_limit', 1), # 从设置或默认值获取并发限制
                'batch_size': task_settings.get('batch_size'),
                'stop_event': stop_event
            }
            execute_method = None

            if mode == "全量导入":
                 kwargs['start_date'] = '19900101'
                 kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                 execute_method = task.execute
            elif mode == "智能增量":
                 #智能增量现在也需要传递回调
                 execute_method = task.smart_incremental_update 
                 # smart_incremental_update 会把 kwargs 传给 execute, 所以回调也会被传递
            elif mode == "手动增量":
                 if not start_date_str:
                     raise ValueError("手动增量模式需要指定开始日期")
                 kwargs['start_date'] = start_date_str.replace('-', '')
                 kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                 execute_method = task.execute
            else:
                raise ValueError(f"未知的执行模式: {mode}")

            if not execute_method:
                 raise NotImplementedError(f"任务 {task_name} 不支持模式 {mode}")

            # 执行任务方法, 传递 kwargs (包含回调)
            result = await execute_method(**kwargs)
            
            tasks_executed_count += 1
            status = result.get('status', 'unknown') if isinstance(result, dict) else 'unknown_result'
            
            if status in ['success', 'partial_success', 'up_to_date', 'no_data']:
                tasks_succeeded_count += 1
                final_status = '完成' if status != 'partial_success' else '部分完成'
                _running_task_status[task_name]['status'] = final_status
                # Ensure final progress is 100% on success/completion
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
    _current_stop_event = None

# --- 设置处理 ---
def _load_settings() -> Dict:
    """加载配置文件 (config.json) - 现在从用户配置目录加载"""
    try:
        # 确保在使用前打印或记录最终的 CONFIG_FILE_PATH 以便调试
        logging.info(f"尝试从用户配置路径加载设置: {CONFIG_FILE_PATH}")
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            settings = json.load(f)
            logging.info(f"从 {CONFIG_FILE_PATH} 加载设置成功。")
            return settings
    except FileNotFoundError:
        logging.warning(f"配置文件 {CONFIG_FILE_PATH} 未找到，将使用空设置。")
        return {}
    except json.JSONDecodeError:
        logging.error(f"解析配置文件 {CONFIG_FILE_PATH} 失败。文件可能已损坏。")
        return {} # 返回空字典而不是抛出异常
    except Exception as e:
        logging.exception(f"加载配置文件时发生未知错误: {CONFIG_FILE_PATH}")
        return {}

def _save_settings(settings: Dict) -> bool:
    """保存设置到配置文件 (config.json) - 现在保存到用户配置目录"""
    try:
        # 确保目录存在 (这是关键步骤)
        logging.info(f"尝试保存设置到用户配置路径: {CONFIG_FILE_PATH}")
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH), exist_ok=True)
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4, ensure_ascii=False)
        logging.info(f"设置已成功保存到 {CONFIG_FILE_PATH}")
        # 保存成功后不再发送 LOG_ENTRY，由 _perform_save_settings 或 _handle_save_settings 发送状态
        return True
    except IOError as e:
        logging.error(f"写入配置文件 {CONFIG_FILE_PATH} 时出错: {e}")
        # 保存失败时也不发送 ERROR，让上层函数处理错误报告
        # response_queue.put(('ERROR', f"保存配置失败: 文件写入错误 ({e})。"))
        return False
    except Exception as e:
        logging.exception(f"保存配置文件时发生未知错误: {CONFIG_FILE_PATH}")
        # response_queue.put(('ERROR', f"保存配置失败: 未知错误 ({e})。"))
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
    """Load settings from config file or return defaults."""
    try:
        settings = _load_settings()
        # 确保返回的字典结构完整，即使文件为空或部分缺失
        # 这样 GUI 端就不需要处理 None 或 KeyError
        default_settings = {
            "database": {"url": ""}, 
            "api": {"tushare_token": ""}
        }
        # 合并加载的设置和默认设置，加载的优先
        # 注意：这只是浅层合并
        merged_settings = default_settings.copy()
        if isinstance(settings, dict):
            if "database" in settings and isinstance(settings["database"], dict):
                 merged_settings["database"].update(settings["database"])
            if "api" in settings and isinstance(settings["api"], dict):
                 merged_settings["api"].update(settings["api"])
        else:
            # 如果 _load_settings 返回的不是字典（例如 None 或异常被捕获返回空），则返回默认
            logging.warning("无法从 config.json 加载有效设置，返回默认空设置。")
            return default_settings
            
        # 清理 None 值，替换为空字符串，以便 GUI 显示
        if merged_settings["database"].get("url") is None:
            merged_settings["database"]["url"] = ""
        if merged_settings["api"].get("tushare_token") is None:
            merged_settings["api"]["tushare_token"] = ""
            
        return merged_settings
    except Exception as e:
        logging.error(f"获取当前设置时出错: {e}")
        # 发生任何异常，都返回安全的默认值
        return {
            "database": {"url": ""}, 
            "api": {"tushare_token": ""}
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

def _fail_all_running_tasks():
    # Implementation of _fail_all_running_tasks method
    pass
