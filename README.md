# lyxyImage · Halo 封面图插件

为 Halo 文章提供封面图能力：随机图、文章首图、自定义 SVG、OpenAI 兼容 AI 生成。

- 作者：临渊羡鱼
- 站点：https://blog.994613.xyz
- 授权：GPL-3.0
- 兼容：Halo >= 2.23（已在 Halo 2.25.x / Spring Boot 4 环境验证）

## 功能

| 能力 | 说明 |
| --- | --- |
| 随机图 | 六路图源自动降级，默认首选 `t.alcy.cc/ycy` |
| 文章首图 | 从文章正文（Markdown / HTML）提取第一张图片作为封面 |
| 自定义 SVG | 按文章注解生成带标题、配色与 ICON 的封面 |
| AI 生成 | OpenAI 兼容的 `/images/generations` 接口，支持 `url` 与 `b64_json` 返回 |
| 发布自动补图 | 文章发布时若无封面，自动按注解里的封面图来源生成并回写 |

## 图源降级链

随机图按以下优先级串行尝试，前一个失败才请求下一个，**全部失败才报错**：

1. `https://t.alcy.cc/ycy/`（默认首选）
2. `https://t.alcy.cc/moe/`
3. `https://t.alcy.cc/pc/`
4. `https://www.dmoe.cc/random.php?return=302`
5. Bing 官方 `HPImageArchive` 接口
6. Bing 代理 `api.xsot.cn`

下载后会按 magic number 校验真实图片格式（JPEG / PNG / GIF / WebP / BMP），
空响应、防外链占位图、HTML 均判定为失败并自动降级，不会写出损坏的封面文件。

## 使用

1. 安装插件并启用。
2. 插件设置 → 基本设置：选择**附件存储策略**（封面图转存目标）。
3. 插件设置 → AI 生成设置：填写 OpenAI 兼容的 `API 基础地址`、`API Key`、`模型名称`
   （请求地址为 `{基础地址}/images/generations`）。
4. 文章编辑页右侧「其他」中选择封面图来源（文章获取首图 / 随机图 / 自定义 / AI 生成），
   或直接使用编辑器工具栏的「生成封面图」。

## 构建

```bash
# 需要 JDK 21
./gradlew build
# 产物：build/libs/plugin-lyxyImage-<version>.jar
```

前端位于 `ui/`，基于 rsbuild + `@halo-dev/ui-plugin-bundler-kit`，构建时由 Gradle 自动执行。

## 目录结构

```
src/main/java/cc/lyxy/image/     后端（端点、服务、图源、AI 生成）
src/main/resources/
  plugin.yaml                    插件描述
  extensions/settings.yaml       插件设置表单
  extensions/annotation-setting.yaml  文章注解表单
  extensions/reverseProxy.yaml   静态资源反代
ui/src/                          控制台 UI 扩展
```

## 许可

GPL-3.0，详见 [LICENSE](LICENSE)。
