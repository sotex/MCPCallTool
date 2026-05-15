import asyncio
import threading
from datetime import timedelta
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

# 尝试导入 ExceptionGroup 以兼容旧版本 Python
try:
    _ExceptionGroup = ExceptionGroup
except NameError:
    from anyio import ExceptionGroup as _ExceptionGroup

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mcp_secure_key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

mcp_registry = {}
mcp_loop = asyncio.new_event_loop()


def start_async_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=start_async_loop, args=(mcp_loop,), daemon=True).start()


async def mcp_service_worker(url, result_event, result_container):
    """
    参考示例代码：
    1. 使用 (read, write) 元组解包。
    2. 对 ClientSession 使用 async with 上下文管理，确保初始化和关闭逻辑正确。
    """
    try:

        def message_handler(message):
            print(f"Received message: {message}")

        # 1. 建立传输层连接
        async with streamable_http_client(url) as (read, write, get_session_id):
            print(f"get_session_id: {get_session_id()}")
            # 2. 建立会话层连接（使用 async with 确保 session 生命周期管理）
            async with ClientSession(read, write, read_timeout_seconds=timedelta(minutes=600), message_handler=message_handler) as session:
                await session.initialize()

                # 3. 获取工具列表
                tools_result = await session.list_tools()
                tools_list = [
                    {
                        "name": t.name,
                        "description": t.description,
                        "inputSchema": t.inputSchema
                    }
                    for t in tools_result.tools
                ]

                # 4. 更新全局注册表
                mcp_registry[url] = {
                    "session": session,  # 注意：session 在 with 块外会失效，但我们通过 loop 维持在此处
                    "tools": tools_list,
                    "active": True
                }

                # 5. 通知 Flask 线程初始化成功
                result_container['tools'] = tools_list
                result_event.set()

                # 6. 核心：阻塞在此处，保持连接和 Session 活跃
                # 只有当外部将 active 设为 False 时，才退出 with 块，从而断开连接
                while mcp_registry.get(url, {}).get('active'):
                    await asyncio.sleep(0.5)

                print(f"MCP Service at {url} is shutting down.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        # 捕获 TaskGroup 或其他网络异常
        error_msg = str(e)
        if "ExceptionGroup" in error_msg or "TaskGroup" in error_msg:
            # 尝试获取内部具体的异常
            error_msg = "连接失败，请检查服务是否支持 SSE 或 URL 是否正确"

        print(f"Worker Error: {error_msg}")
        result_container['error'] = error_msg
        result_event.set()
        if url in mcp_registry:
            mcp_registry[url]['active'] = False


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory("web", filename)


@app.route('/add_service', methods=['POST'])
def add_service():
    url = request.json.get('url').strip()
    if not url:
        return jsonify({"status": "error", "message": "URL 不能为空"}), 400

    result_container = {}
    result_event = threading.Event()

    asyncio.run_coroutine_threadsafe(
        mcp_service_worker(url, result_event, result_container),
        mcp_loop
    )

    # 稍微延长等待时间，有些 MCP 服务启动较慢
    if not result_event.wait(timeout=15):
        return jsonify({"status": "error", "message": "MCP 服务响应超时，请检查 URL 是否正确"}), 504

    if 'error' in result_container:
        return jsonify({"status": "error", "message": result_container['error']}), 500

    return jsonify({"status": "success", "tools": result_container['tools'], "url": url})


@socketio.on('execute_tool')
def handle_execute(data):
    url = data.get('url')
    tool_name = data.get('tool_name')
    args = data.get('arguments', {})

    async def do_call():
        reg = mcp_registry.get(url)
        if not reg or not reg.get('active'):
            socketio.emit('error', {'message': '服务未连接或已断开'})
            return

        session = reg['session']

        try:
            # 修正点：移除 on_progress 参数
            # 如果你的 SDK 版本不支持，强行传递会导致 TypeError
            result = await session.call_tool(
                tool_name,
                arguments=args
            )

            # 格式化输出内容
            formatted_content = []
            for item in result.content:
                # 兼容不同版本的返回对象
                if hasattr(item, 'dict'):
                    formatted_content.append(item.dict())
                elif hasattr(item, 'text'):
                    # 针对 TextContent 对象
                    formatted_content.append({"type": "text", "text": item.text})
                elif hasattr(item, 'data'):
                    # 针对 ImageContent 对象
                    formatted_content.append({
                        "type": "image",
                        "data": item.data,
                        "mimeType": getattr(item, 'mimeType', 'image/png')
                    })
                else:
                    formatted_content.append({"type": "text", "text": str(item)})

            socketio.emit('tool_result', {'content': formatted_content})
        except Exception as e:
            import traceback
            traceback.print_exc()  # 在终端打印完整堆栈方便调试
            socketio.emit('error', {'message': f"执行失败: {str(e)}"})

    asyncio.run_coroutine_threadsafe(do_call(), mcp_loop)


if __name__ == '__main__':
    # 关闭 reloader 避免多进程导致的端口和线程冲突
    socketio.run(app, host='127.0.0.1', port=5000, debug=True, use_reloader=False)
