package cc.lyxy.image.service.impl;

import cc.lyxy.image.constant.ImageConstants;
import cc.lyxy.image.service.ImageTransferService;
import cc.lyxy.image.util.ImageUtils;
import cc.lyxy.image.service.SettingConfigGetter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;
import run.halo.app.core.extension.attachment.Attachment;
import run.halo.app.core.extension.attachment.endpoint.SimpleFilePart;
import run.halo.app.core.extension.content.Post;
import run.halo.app.core.extension.service.AttachmentService;
import run.halo.app.extension.ReactiveExtensionClient;
import run.halo.app.core.extension.User;

import java.net.URI;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class ImageTransferServiceImpl implements ImageTransferService {
    private final SettingConfigGetter settingConfigGetter;
    private final AttachmentService attachmentService;
    private final WebClient.Builder webClientBuilder;
    private final ReactiveExtensionClient client;
    private final DataBufferFactory dataBufferFactory = DefaultDataBufferFactory.sharedInstance;
    private static final Pattern IMAGE_URL_PATTERN = Pattern.compile(".*\\.(jpg|jpeg|png|gif|webp|bmp)$", Pattern.CASE_INSENSITIVE);
    private static final String BROWSER_USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            + "Chrome/120.0.0.0 Safari/537.36";
    /** 小于该字节数的响应视为占位图/空响应 */
    private static final int MIN_IMAGE_BYTES = 10 * 1024;

    @Override
    public Mono<String> updateFile(String picUrl, Post post) {
        return getCurrentUser(post.getSpec().getOwner())
            .flatMap(user -> {
                log.info("用户[{}]开始处理图片: {}", user.getMetadata().getName(), picUrl);
                return settingConfigGetter.getBasicConfig()
                    .switchIfEmpty(Mono.error(new RuntimeException("无法获取基本配置")))
                    .flatMap(config -> {
                        // 检查配置是否完整
                        if (config.getFilePolicy() == null || config.getFilePolicy().isEmpty()) {
                            log.warn("未配置附件存储策略，直接使用原始URL: {}", picUrl);
                            return Mono.just(picUrl);
                        }

                        log.info("开始下载图片: {}", picUrl);

                        return downloadImageBytes(webClientBuilder.build(), picUrl)
                            .flatMap(imageBytes -> {
                                // 按 magic number 识别真实格式，避免把 HTML/空内容当图片上传
                                final MediaType mediaType = ImageUtils.detectMediaType(imageBytes);
                                final String extension = ImageUtils.detectExtension(imageBytes);
                                final String fileName =
                                    ImageConstants.COVER_PREFIX + System.currentTimeMillis() + extension;
                                DataBuffer dataBuffer = dataBufferFactory.wrap(imageBytes);
                                Flux<DataBuffer> dataBufferFlux = Flux.just(dataBuffer);

                                log.info("图片下载完成，文件名: {}, 媒体类型: {}, 大小: {} B",
                                    fileName, mediaType, imageBytes.length);
                                log.info("开始上传图片到存储，策略: {}, 分组: {}",
                                    config.getFilePolicy(), config.getFileGroup());

                                var file = new SimpleFilePart(fileName, dataBufferFlux, mediaType);
                                return attachmentService.upload(user.getMetadata().getName(),
                                        config.getFilePolicy(), config.getFileGroup(), file, null)
                                    .subscribeOn(Schedulers.boundedElastic());
                            })
                            .handle(uploadReturn())
                            .onErrorResume(e -> {
                                log.error("图片转存失败，使用原始URL: {}, 错误: {}", picUrl, e.getMessage(), e);
                                return Mono.just(picUrl);
                            });
                    });
            });
    }

    private BiConsumer<Attachment, SynchronousSink<String>> uploadReturn() {
        return (uploadedAttachment, sink) -> {
            if (uploadedAttachment != null && uploadedAttachment.getStatus() != null
                && uploadedAttachment.getStatus().getPermalink() != null) {
                String uploadedUri = uploadedAttachment.getStatus().getPermalink();
                log.info("图片上传成功: {}", uploadedUri);
                sink.next(uploadedUri);
            } else {
                sink.error(new IllegalStateException("图片上传成功但无法获取URI"));
            }
        };
    }

    @Override
    public Mono<String> updateFile(Flux<DataBuffer> dataBufferFlux, Post post, String filename, MediaType mediaType) {
        return getCurrentUser(post.getSpec().getOwner())
            .flatMap(user -> {
                log.info("用户[{}]开始处理图片数据流: {}", user.getMetadata().getName(), filename);
                return settingConfigGetter.getBasicConfig()
                    .switchIfEmpty(Mono.error(new RuntimeException("无法获取基本配置")))
                    .flatMap(config -> {
                        // 检查配置是否完整
                        if (config.getFilePolicy() == null || config.getFilePolicy().isEmpty()) {
                            log.warn("未配置附件存储策略，请在插件设置中配置");
                            return Mono.error(new RuntimeException("请先在插件设置中配置附件存储策略"));
                        }
                        
                        var file = new SimpleFilePart(filename, dataBufferFlux, mediaType);
                        log.info("开始上传图片到存储，策略: {}, 分组: {}", config.getFilePolicy(), config.getFileGroup());
                        return attachmentService.upload(user.getMetadata().getName(), config.getFilePolicy(), config.getFileGroup(), file, null)
                            .subscribeOn(Schedulers.boundedElastic());
                    })
                    .handle(uploadReturn())
                    .onErrorResume(e -> {
                        log.error("图片处理失败: {}", e.getMessage());
                        return Mono.error(e);
                    });
            });
    }

    private Mono<User> getCurrentUser(String userName) {
        return client.get(User.class, userName)
            .doOnError(e -> log.error("获取用户[{}]信息失败: {}", userName, e.getMessage()));
    }

    private boolean isImageUrl(String url) {
        return url != null && IMAGE_URL_PATTERN.matcher(url).matches();
    }

    /**
     * 下载远程图片并校验内容，返回校验通过的原始字节。
     * 图片防盗链/失效时常见返回 200 + 空 body 或占位图，这里统一做校验并报错。
     */
    private Mono<byte[]> downloadImageBytes(WebClient webClient, String url) {
        final String referer = ImageUtils.refererOf(url);
        return webClient.get()
            .uri(URI.create(url))
            .header("User-Agent", BROWSER_USER_AGENT)
            .header("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
            .header("Referer", referer)
            .retrieve()
            .onStatus(HttpStatusCode::isError, response ->
                response.bodyToMono(String.class)
                    .defaultIfEmpty("")
                    .flatMap(errorBody -> {
                        log.error("下载图片失败，状态码: {}，URL: {}", response.statusCode(), url);
                        return response.releaseBody().then(Mono.error(new RuntimeException(
                            "下载图片失败，状态码: " + response.statusCode())));
                    }))
            .bodyToMono(byte[].class)
            .flatMap(bytes -> {
                if (bytes == null || bytes.length < MIN_IMAGE_BYTES) {
                    int size = bytes == null ? 0 : bytes.length;
                    log.error("图片内容异常（{} B），可能是防盗链或空响应，URL: {}", size, url);
                    return Mono.error(new IllegalStateException(
                        "图片内容异常（" + size + " 字节），可能是防盗链或链接已失效"));
                }
                if (!ImageUtils.isImage(bytes)) {
                    log.error("返回内容不是有效图片（{} B），URL: {}", bytes.length, url);
                    return Mono.error(new IllegalStateException("返回内容不是有效图片"));
                }
                return Mono.just(bytes);
            })
            .doOnError(e -> log.error("下载图片失败: {}, URL: {}", e.getMessage(), url));
    }
} 