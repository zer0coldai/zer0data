# Docker 部署

本文档基于 MkDocs Material 官方 Docker 镜像 `squidfunk/mkdocs-material`。

## 本地预览（Docker Compose）

在仓库根目录执行：

```bash
docker compose -f sdk/docker-compose.docs.yml up docs-serve
```

访问：`http://127.0.0.1:8000`

## 构建静态站点（Docker Compose）

```bash
docker compose -f sdk/docker-compose.docs.yml run --rm docs-build
```

构建产物目录：`sdk/site/`

## 生产部署（Docker + Nginx）

先构建：

```bash
docker compose -f sdk/docker-compose.docs.yml run --rm docs-build
```

再发布静态站：

```bash
docker compose -f sdk/docker-compose.docs.yml up -d docs-static
```

访问：`http://127.0.0.1:8080`

## 使用官方单条命令（可选）

在仓库根目录执行：

```bash
docker run --rm -it -p 8000:8000 -v ${PWD}/sdk:/docs squidfunk/mkdocs-material:latest serve -f mkdocs.yml -a 0.0.0.0:8000
```

## CI/CD 建议

- 本地/CI 构建统一使用 `squidfunk/mkdocs-material` 镜像
- CI 构建阶段使用 `docs-build`
- 发布阶段可选：
  - 将 `sdk/site/` 上传到 Pages / 对象存储
  - 或通过 `docs-static` 容器直接托管
