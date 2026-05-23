# Strategy Service

Python strategy runtime service for Hushine.

## Debugger runtime 本地调试

这个流程用于调试用户本地 debugger runtime 中挂载的策略文件，例如：

```text
/Users/xdy/hushine-debug-workspace/self_hosted_strategy.py
```

容器内对应路径是：

```text
/workspace/self_hosted_strategy.py
```

### 通用前置条件

1. 已经在控制面板创建并启动 `debugger` 类型的 self-hosted runtime。
2. Runtime Management 中已经执行过准备调试，生成本地 workspace 和策略模板。
3. Account 页面选择该 debugger runtime 后，已经点击 `Run Debugger`，让 control-panel 把调试数据集下发到 runtime。
4. IDE 使用本地目录作为项目打开：

```bash
/Users/xdy/hushine-debug-workspace
```

## VSCode 调试

VSCode 当前是推荐的本地调试方式，配置更直接：容器内 `debugpy` 监听端口，VSCode 主动 attach 进去。

### 容器启动要求

VSCode/debugpy 模式需要发布调试端口。启动 debugger runtime 时需要包含：

```bash
-p 127.0.0.1:5678:5678
```

完整示例：

```bash
docker run --rm -it \
  --name hushine-debugger \
  -p 127.0.0.1:5678:5678 \
  -v $HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \
  -v $HOME/hushine-debug-workspace:/workspace \
  -e RUNTIME_INGRESS_MODE=outbound \
  -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \
  -e CONTROL_PANEL_SERVICE_GRPC_ADDR=host.docker.internal:50054 \
  hushine/strategy-runtime:debugger-dev
```

确认端口映射：

```bash
docker ps --filter name=hushine-debugger
```

需要看到类似：

```text
127.0.0.1:5678->5678/tcp
```

### VSCode 配置

打开本地 workspace：

```bash
code /Users/xdy/hushine-debug-workspace
```

确认 `.vscode/launch.json` 存在，内容类似：

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Hushine Debugger Attach",
      "type": "debugpy",
      "request": "attach",
      "connect": {
        "host": "localhost",
        "port": 5678
      },
      "pathMappings": [
        {
          "localRoot": "${workspaceFolder}",
          "remoteRoot": "/workspace"
        }
      ]
    }
  ]
}
```

### 启动 replay

在 debugger runtime 容器中执行：

```bash
docker exec -it hushine-debugger hushine-debug replay \
  --debugpy \
  --host 0.0.0.0 \
  --port 5678 \
  --wait
```

`--wait` 会让 replay 等待 VSCode attach 后再开始跑数据。然后在 VSCode 的 `Run and Debug` 中选择：

```text
Hushine Debugger Attach
```

点击启动。连接成功后，replay 开始执行，断点会在 `self_hosted_strategy.py` 中命中。

### VSCode 调试原理

VSCode/debugpy 链路是：

```text
VSCode localhost:5678
  -> Docker port mapping
  -> container 0.0.0.0:5678
  -> debugpy
  -> Python replay thread
  -> /workspace/self_hosted_strategy.py
```

数据仍然由 control-panel 通过 RuntimeChannel 下发到 runtime。VSCode 只负责接管 Python 调试，不负责传输行情数据。

## PyCharm 调试

PyCharm 使用 Python Debug Server，容器里的 Python 主动连接 Mac 上的 PyCharm。

### PyCharm 配置

在 PyCharm 里创建 `Python Debug Server` 配置：

- Host: `0.0.0.0` 或本机默认监听地址
- Port: `5680`
- Path mapping:

```text
Local path:  /Users/xdy/hushine-debug-workspace
Remote path: /workspace
```

先启动这个 Debug Server，让 PyCharm 进入等待连接状态。

### 启动 replay

在 debugger runtime 容器中执行：

```bash
docker exec -it hushine-debugger hushine-debug replay \
  --pycharm \
  --host host.docker.internal \
  --port 5680
```

如果容器名不是 `hushine-debugger`，先用下面命令确认：

```bash
docker ps --format '{{.Names}} {{.Image}} {{.Status}}'
```

### PyCharm 调试原理

PyCharm 调试链路是反向连接：

```text
container Python process
  -> pydevd_pycharm.settrace()
  -> host.docker.internal:5680
  -> PyCharm Debug Server
```

因此 PyCharm 模式下不需要给容器映射调试端口。容器里的 Python 主动连接 Mac 上的 PyCharm，PyCharm 只负责接管 Python trace hook 和断点。

数据链路和调试链路是分开的：

```text
页面 Run Debugger
  -> control-panel-service
  -> RuntimeChannel
  -> debugger runtime 缓存调试数据集

hushine-debug replay
  -> 触发 runtime 使用缓存数据集重新回放
```

PyCharm 不负责传输行情数据，只负责 Python 断点调试。

## 断点命中条件

策略代码通过动态加载执行。为了让 PyCharm 能匹配断点，运行时会使用真实策略路径编译代码：

```python
compile(strategy_code, "/workspace/self_hosted_strategy.py", "exec")
```

所以断点能否命中，取决于 PyCharm path mapping 是否把：

```text
/workspace/self_hosted_strategy.py
```

正确映射到：

```text
/Users/xdy/hushine-debug-workspace/self_hosted_strategy.py
```

## 常见问题

#### Address already in use

说明 PyCharm Debug Server 端口已经被占用。处理方式：

1. 停掉旧的 PyCharm Debug Server。
2. 或换一个端口，例如 `5681`，并同步修改 replay 命令：

```bash
docker exec -it hushine-debugger hushine-debug replay \
  --pycharm \
  --host host.docker.internal \
  --port 5681
```

#### Connected 但断点不生效

优先检查 path mapping：

```text
Local path:  /Users/xdy/hushine-debug-workspace
Remote path: /workspace
```

如果 PyCharm 提示找不到：

```text
/app/strategy-service/...
```

这通常是框架内部代码路径，不影响只调试用户策略。调试用户策略时，只需要确保 `/workspace` 映射正确。

VSCode 下还需要确认打开的是：

```text
/Users/xdy/hushine-debug-workspace
```

而不是整个 hushine 仓库。

#### VSCode 连接失败

先确认容器端口映射存在：

```bash
docker ps --filter name=hushine-debugger
```

需要看到：

```text
127.0.0.1:5678->5678/tcp
```

如果 replay 已经结束，需要重新触发：

```bash
docker exec -it hushine-debugger hushine-debug replay \
  --debugpy \
  --host 0.0.0.0 \
  --port 5678 \
  --wait
```

#### wrong debugger version

debugger runtime 镜像需要安装和 PyCharm 版本匹配的 `pydevd-pycharm`。当前 debugger 镜像使用 Python 3.12，并安装：

```text
pydevd-pycharm~=252.26199.168
```

如果 PyCharm 升级后再次出现版本告警，需要同步调整镜像中的 `PYDEVD_PYCHARM_VERSION` 并重建 debugger runtime 镜像。

#### 第二次 replay 失败

如果出现旧连接残留，先停止 PyCharm Debug Server，再重新启动 Debug Server，然后重新执行 replay 命令。当前 runtime 会在 replay 结束后主动清理 PyCharm debugger 状态，但 IDE 侧端口仍可能被旧配置占用。

## VSCode 与 PyCharm 的差异

VSCode 模式是 IDE 主动连接容器：

```text
VSCode -> container
```

因此 VSCode 需要 Docker 端口映射，例如：

```text
-p 127.0.0.1:5678:5678
```

PyCharm 模式是容器主动连接 IDE：

```text
container -> PyCharm
```

因此 PyCharm 不需要发布容器调试端口。
