# 安装指南

## 前置条件

- Python `3.11+`
- 可访问的 ClickHouse 服务（HTTP 端口默认 `8123`）
- 对仓库有读取权限（SSH 或 HTTPS）

## 从 Git 子目录安装

从指定提交安装（可复现）：

```bash
pip install "git+ssh://git@github.com/zer0coldai/zer0data.git@bd74466#subdirectory=sdk"
```

从 `main` 分支安装：

```bash
pip install "git+ssh://git@github.com/zer0coldai/zer0data.git@main#subdirectory=sdk"
```

无 SSH key 可用 HTTPS：

```bash
pip install "git+https://github.com/zer0coldai/zer0data.git@main#subdirectory=sdk"
```

## 验证安装

```bash
python -c "from zer0data import Client; print(Client.from_env)"
```
