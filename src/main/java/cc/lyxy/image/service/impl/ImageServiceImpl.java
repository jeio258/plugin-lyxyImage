package cc.lyxy.image.service.impl;

import cc.lyxy.image.constant.ImageConstants;
import cc.lyxy.image.model.ImageType;
import cc.lyxy.image.service.AIImageGenerator;
import cc.lyxy.image.service.CoverImageGenerator;
import cc.lyxy.image.service.ImageService;
import cc.lyxy.image.service.ImageTransferService;
import cc.lyxy.image.service.SettingConfigGetter;
import cc.lyxy.image.util.ImageUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.halo.app.content.PostContentService;
import run.halo.app.core.extension.content.Post;
import run.halo.app.extension.ReactiveExtensionClient;

@Slf4j
@Service
@RequiredArgsConstructor
public class ImageServiceImpl implements ImageService, InitializingBean {
    private final SettingConfigGetter settingConfigGetter;
    private final PostContentService postContentService;
    private final ImageTransferService imageTransferService;
    private final WebClient.Builder webClientBuilder;
    private final ObjectMapper objectMapper;
    private final CoverImageGenerator coverImageGenerator;
    private Map<String, AIImageGenerator> aiImageGeneratorMap;
    private final List<AIImageGenerator> aiImageGenerators;
    private final ReactiveExtensionClient client;

    private static final String BROWSER_USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            + "Chrome/120.0.0.0 Safari/537.36";

    /**
     * 图源优先级链（已剔除全部失效图源）。
     * 默认首选 https://t.alcy.cc/ycy，失败后按顺序逐个降级；
     * 只有整条链全部失败时才报错，避免单个图源挂掉就完全不可用。
     */
    private static final String SOURCE_YCY = "https://t.alcy.cc/ycy/";
    private static final String SOURCE_MOE = "https://t.alcy.cc/moe/";
    private static final String SOURCE_PC = "https://t.alcy.cc/pc/";
    private static final String SOURCE_DMOE = "https://www.dmoe.cc/random.php?return=302";
    /** Bing 每日壁纸：官方 JSON 接口 */
    private static final String BING_OFFICIAL_API =
        "https://www.bing.com/HPImageArchive.aspx?format=js&idx=0&n=1&mkt=zh-CN";
    /** Bing 每日壁纸：第三方代理接口（官接口出问题时兑底） */
    private static final String BING_FALLBACK_API =
        "https://api.xsot.cn/bing/?quality=1920x1080&mkt=zh-cn";
    private final DataBufferFactory dataBufferFactory = DefaultDataBufferFactory.sharedInstance;
    private static final int BUFFER_SIZE = 8192;
    @Override
    public Mono<String> processRandomImage(Post post) {
        log.info("开始处理随机图片，文章标题: {}", post.getSpec().getTitle());
        return settingConfigGetter.getBasicConfig()
            .switchIfEmpty(Mono.error(new IllegalStateException("无法获取基本配置")))
            .flatMap(config -> {
                String randomType = config.getRandomType();
                log.info("随机图类型配置: {}（图源固定为优先级链，默认首选 ycy）", randomType);
                return firstSuccess(buildSourceChain(post), randomType);
            })
            .doOnError(e -> log.error("处理随机图片过程中发生错误: {}", e.getMessage()));
    }

    /**
     * 按优先级构建图源链：ycy → moe → pc → dmoe → Bing 官方 → Bing 代理。
     * 构建过程不发请求，实际请求由 concatMap 串行触发，保证前一个失败才走下一个。
     */
    private List<Mono<String>> buildSourceChain(Post post) {
        return List.of(
            imageTransferService.updateFile(SOURCE_YCY, post),
            imageTransferService.updateFile(SOURCE_MOE, post),
            imageTransferService.updateFile(SOURCE_PC, post),
            imageTransferService.updateFile(SOURCE_DMOE, post),
            fetchBingOfficialImageUrl().flatMap(url -> imageTransferService.updateFile(url, post)),
            fetchBingFallbackImageUrl().flatMap(url -> imageTransferService.updateFile(url, post))
        );
    }

    /**
     * 按顺序尝试图源链，返回第一个成功结果；全部失败才报错
     */
    private Mono<String> firstSuccess(List<Mono<String>> candidates, String randomType) {
        return Flux.fromIterable(candidates)
            .concatMap(mono -> mono.onErrorResume(e -> {
                log.warn("[{}] 图源失败，降级到下一个: {}", randomType, e.getMessage());
                return Mono.empty();
            }))
            .next()
            .switchIfEmpty(Mono.error(new IllegalStateException(
                "全部随机图源均不可用，请检查 NAS 外网连通性")));
    }

    /**
     * Bing 官方每日壁纸
     */
    private Mono<String> fetchBingOfficialImageUrl() {
        return webClientBuilder.build().get()
            .uri(BING_OFFICIAL_API)
            .header("User-Agent", BROWSER_USER_AGENT)
            .retrieve()
            .bodyToMono(String.class)
            .flatMap(body -> {
                try {
                    JsonNode images = objectMapper.readTree(body).path("images");
                    if (images.isArray() && !images.isEmpty()) {
                        String url = images.get(0).path("url").asText("");
                        if (!url.isEmpty()) {
                            String fullUrl = url.startsWith("http")
                                ? url : "https://www.bing.com" + url;
                            log.info("Bing 官方壁纸: {}", fullUrl);
                            return Mono.just(fullUrl);
                        }
                    }
                    return Mono.error(new IllegalStateException("Bing 官方接口未返回图片"));
                } catch (Exception e) {
                    return Mono.error(
                        new IllegalStateException("解析 Bing 官方响应失败: " + e.getMessage()));
                }
            });
    }

    /**
     * Bing 备用图源
     */
    private Mono<String> fetchBingFallbackImageUrl() {
        return webClientBuilder.build().get()
            .uri(BING_FALLBACK_API)
            .header("User-Agent", BROWSER_USER_AGENT)
            .retrieve()
            .bodyToMono(String.class)
            .flatMap(body -> {
                try {
                    String url = objectMapper.readTree(body).path("data").path("image").asText("");
                    if (!url.isEmpty()) {
                        log.info("Bing 备用壁纸: {}", url);
                        return Mono.just(url);
                    }
                    return Mono.error(new IllegalStateException("Bing 备用接口未返回图片"));
                } catch (Exception e) {
                    return Mono.error(
                        new IllegalStateException("解析 Bing 备用响应失败: " + e.getMessage()));
                }
            });
    }

    @Override
    public Mono<String> processFirstPostImage(Post post) {
        String postName = post.getMetadata().getName();
        log.info("开始提取文章首图，文章: {}", post.getSpec().getTitle());
        
        // 优先获取草稿内容，如果没有再获取已发布内容
        return postContentService.getHeadContent(postName)
            .switchIfEmpty(postContentService.getReleaseContent(postName))
            .doOnNext(contentWrapper -> {
                log.info("获取到文章内容，类型: {}, 内容长度: {}", 
                    contentWrapper.getRawType(), 
                    contentWrapper.getRaw() != null ? contentWrapper.getRaw().length() : 0);
            })
            .flatMap(contentWrapper -> {
                String content = contentWrapper.getRaw();
                String rawType = contentWrapper.getRawType();
                
                if (content == null || content.isEmpty()) {
                    log.warn("文章内容为空");
                    return Mono.error(new IllegalStateException("文章内容为空"));
                }
                
                String firstImgSrc;
                if ("markdown".equals(rawType)) {
                    firstImgSrc = extractMarkdownImage(content);
                    log.info("Markdown 格式，提取到图片: {}", firstImgSrc);
                } else {
                    firstImgSrc = extractHtmlImage(content);
                    log.info("HTML 格式，提取到图片: {}", firstImgSrc);
                }
                
                if (firstImgSrc == null || firstImgSrc.isEmpty()) {
                    return Mono.error(new IllegalStateException("未找到文章中的图片"));
                }
                return Mono.just(firstImgSrc);
            })
            .switchIfEmpty(Mono.error(new IllegalStateException("无法获取文章内容")))
            .onErrorResume(e -> {
                log.error("提取文章图片失败: {}", e.getMessage());
                return Mono.error(new ServerWebInputException("获取文章内容时出错: " + e.getMessage()));
            });
    }

    @Override
    public Mono<String> processCustomizeImage(Post post) {
        log.info("使用自定义图片策略处理文章: {}", post.getSpec().getTitle());
        try {
            return coverImageGenerator.generateCoverImage(post);
        } catch (IOException e) {
            log.error("生成封面图片失败", e);
            return Mono.error(e);
        }
    }

    private String extractMarkdownImage(String content) {
        Pattern pattern = Pattern.compile("!\\[(.*?)]\\((.*?)\\)");
        Matcher matcher = pattern.matcher(content);
        return matcher.find() ? matcher.group(2) : null;
    }

    private String extractHtmlImage(String content) {
        try {
            Document doc = Jsoup.parse(content);
            var imgElement = doc.selectFirst("img[src]");
            
            if (imgElement == null) {
                log.warn("HTML 中没有找到 img 标签");
                return null;
            }
            
            String src = imgElement.attr("src");
            log.info("提取到图片: {}", src);
            return src;
        } catch (Exception e) {
            log.warn("解析HTML图片失败: {}", e.getMessage());
            return null;
        }
    }
    @Override
    public Mono<String> uploadCoverImage(String imageContent, Post post) {
        if (imageContent == null || imageContent.isEmpty()) {
            return Mono.error(new IllegalArgumentException("图片内容不能为空"));
        }
        
        ImageType imageType = parseImageType(imageContent);
        log.info("上传封面图，图片类型: {}", imageType.getClass().getSimpleName());
        
        return switch (imageType) {
            case ImageType.UrlImage(String url) -> uploadFromUrl(post, url);
            case ImageType.Base64Image(String content, MediaType mediaType) ->
                uploadFromBase64(post, content, mediaType);
            case ImageType.SvgImage(String content) -> uploadSvg(post, content);
            case ImageType.LocalImage(String path) -> {
                // 本地路径已经上传过了，直接返回
                log.info("图片已是本地路径，无需重新上传: {}", path);
                yield Mono.just(path);
            }
            case ImageType.UnknownImage() -> Mono.error(new IllegalArgumentException(
                "无法识别图片格式，请确保是有效的图片URL、Base64或SVG"));
        };
    }

    private Mono<String> uploadFromUrl(Post post, String imageUrl) {
        return Mono.fromCallable(() -> {
                var uri = new URI(imageUrl);
                return new InputStreamResource(uri.toURL().openStream());
            })
            .flatMap(resource -> {
                var dataBufferFlux = DataBufferUtils.read(resource, dataBufferFactory, BUFFER_SIZE);
                return imageTransferService.updateFile(dataBufferFlux, post,
                    ImageConstants.COVER_PREFIX + System.currentTimeMillis() + ImageUtils.getFileExtension(imageUrl),
                    ImageUtils.getMediaTypeFromUrl(imageUrl));
            })
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorResume(e -> {
                log.error("Failed to upload image from URL: {}", imageUrl, e);
                return Mono.error(new RuntimeException("上传图片失败: " + e.getMessage(), e));
            });
    }

    private Mono<String> uploadFromBase64(Post post, String base64Content, MediaType mediaType) {
        return Mono.fromCallable(() -> {
                var parts = base64Content.split(",");
                var base64Data = parts[1];
                var imageData = Base64.getDecoder().decode(base64Data);
                return new InputStreamResource(new ByteArrayInputStream(imageData));
            })
            .flatMap(resource -> {
                var dataBufferFlux = DataBufferUtils.read(resource, dataBufferFactory, BUFFER_SIZE);
                return imageTransferService.updateFile(dataBufferFlux, post,
                    ImageConstants.COVER_PREFIX + System.currentTimeMillis() + ImageUtils.getFileExtensionFromMediaType(mediaType.toString()),
                    mediaType);
            })
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorResume(e -> {
                log.error("Failed to upload image from base64", e);
                return Mono.error(new RuntimeException("上传Base64图片失败: " + e.getMessage(), e));
            });
    }

    private Mono<String> uploadSvg(Post post, String svgContent) {
        var svgInputStream = new ByteArrayInputStream(svgContent.getBytes(StandardCharsets.UTF_8));
        var svgResource = new InputStreamResource(svgInputStream);
        var dataBufferFlux = DataBufferUtils.read(svgResource, dataBufferFactory, BUFFER_SIZE);
        var filename = ImageConstants.COVER_PREFIX + System.currentTimeMillis() + ".svg";
        return imageTransferService.updateFile(dataBufferFlux, post, filename, MediaType.parseMediaType("image/svg+xml"));
    }
    private ImageType parseImageType(String content) {
        log.debug("解析图片类型，内容前50字符: {}", content.length() > 50 ? content.substring(0, 50) : content);
        
        if (content == null || content.isEmpty()) {
            log.warn("图片内容为空");
            return new ImageType.UnknownImage();
        }
        
        // 处理 URL（http 或 https）
        if (content.startsWith("http://") || content.startsWith("https://")) {
            log.debug("识别为 URL 图片");
            return new ImageType.UrlImage(content);
        }
        
        // 处理本地路径（已上传的附件，如 /upload/xxx.jpg）
        if (content.startsWith("/")) {
            log.debug("识别为本地路径图片，无需重新上传");
            return new ImageType.LocalImage(content);
        }
        
        // 处理 Base64
        if (content.startsWith("data:")) {
            var parts = content.split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException(ImageConstants.ERROR_INVALID_BASE64);
            }
            var mediaType = parts[0].split(";")[0].split(":")[1];
            log.debug("识别为 Base64 图片，媒体类型: {}", mediaType);
            return new ImageType.Base64Image(content, MediaType.parseMediaType(mediaType));
        }
        
        // 处理 SVG
        if (content.contains("<svg")) {
            log.debug("识别为 SVG 图片");
            return new ImageType.SvgImage(content);
        }
        
        log.warn("无法识别图片类型，内容: {}", content.length() > 100 ? content.substring(0, 100) + "..." : content);
        return new ImageType.UnknownImage();
    }
    @Override
    public Mono<String> processAIGeneratedImage(Post post, String size, String style, boolean watermark) {
        log.info("使用 AI 生成图片策略处理文章: {}, 尺寸: {}, 风格: {}, 水印: {}",
            post.getSpec().getTitle(), size, style, watermark);
        return settingConfigGetter.getAIConfig()
            .switchIfEmpty(Mono.error(new IllegalStateException("无法获取 AI 配置")))
            .flatMap(config -> {
                String aiProvider = config.getAiProvider();
                log.info("当前使用 AI 生成平台: {}, 支持平台: {}", aiProvider, aiImageGeneratorMap.keySet());
                if (StringUtils.isBlank(aiProvider)) {
                    return Mono.error(new IllegalStateException("未配置 AI 生成平台"));
                }
                AIImageGenerator imageGenerator = aiImageGeneratorMap.get(aiProvider);
                if (Objects.isNull(imageGenerator)) {
                    return Mono.error(new IllegalStateException("未找到对应的 AI 生成平台"));
                }
                return imageGenerator.generateImage(post, size, style, watermark);
            })
            .doOnSuccess(url -> log.info("AI 封面图生成成功: {}", url))
            .doOnError(e -> log.error("AI 封面图生成失败: {}", e.getMessage()));
    }

    @Override
    public void afterPropertiesSet() {
        this.aiImageGeneratorMap = this.aiImageGenerators.stream()
            .collect(Collectors.toMap(AIImageGenerator::supportAiProvider, Function.identity()));
    }
}
