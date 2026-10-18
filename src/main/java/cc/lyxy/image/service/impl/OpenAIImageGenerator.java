package cc.lyxy.image.service.impl;

import cc.lyxy.image.service.SettingConfigGetter.AIConfig;
import cc.lyxy.image.util.ImageUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.halo.app.core.extension.content.Post;

/**
 * OpenAI 兼容格式的图片生成。
 *
 * <p>请求 {@code POST {apiBaseUrl}/images/generations}，兼容 OpenAI Images API 以及
 * 各类宣称"OpenAI 兼容"的服务（如火山方舟、硅基流动、OneAPI/NewAPI 等）。
 * 响应同时兼容 {@code data[0].url} 与 {@code data[0].b64_json} 两种返回形式。</p>
 */
@Slf4j
@Service
public class OpenAIImageGenerator extends AbstractAIImageGenerator {

    /** 唯一可选的生成平台标识，保留以兼容设置项 */
    public static final String PROVIDER = "openai";

    private static final String GENERATIONS_PATH = "/images/generations";

    @Override
    public String supportAiProvider() {
        return PROVIDER;
    }

    @Override
    protected Mono<String> doGenerateImage(AIConfig config, String prompt, String size,
        boolean watermark, Post post) {
        if (StringUtils.isBlank(config.getApiKey())) {
            return Mono.error(new IllegalStateException("未配置 API Key"));
        }
        if (StringUtils.isBlank(config.getApiBaseUrl())) {
            return Mono.error(new IllegalStateException("未配置 API 基础地址"));
        }

        String baseUrl = StringUtils.removeEnd(config.getApiBaseUrl().trim(), "/");
        String apiUrl = baseUrl + GENERATIONS_PATH;
        String model = StringUtils.defaultIfBlank(config.getModel(), "gpt-image-1");
        String actualSize = StringUtils.defaultIfBlank(size, "1024x1024");

        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("model", model);
        requestBody.put("prompt", prompt);
        requestBody.put("size", actualSize);
        requestBody.put("n", 1);

        log.info("调用 OpenAI 兼容图片接口: {}, 模型: {}, 尺寸: {}", apiUrl, model, actualSize);

        WebClient webClient = webClientBuilder.build();

        return webClient.post()
            .uri(apiUrl)
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + config.getApiKey().trim())
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestBody)
            .retrieve()
            .onStatus(status -> status.is4xxClientError(), response -> response.bodyToMono(
                    String.class)
                .defaultIfEmpty("")
                .flatMap(errorBody -> {
                    log.error("图片接口 4xx 响应: {}", errorBody);
                    return Mono.error(
                        new IllegalStateException("API 请求失败 (4xx): " + errorBody));
                }))
            .onStatus(status -> status.is5xxServerError(), response -> response.bodyToMono(
                    String.class)
                .defaultIfEmpty("")
                .flatMap(errorBody -> {
                    log.error("图片接口 5xx 响应: {}", errorBody);
                    return Mono.error(
                        new IllegalStateException("API 服务器错误 (5xx): " + errorBody));
                }))
            .bodyToMono(String.class)
            .flatMap(responseBody -> parseAndUpload(responseBody, post));
    }

    /**
     * 解析响应：优先 url，其次 b64_json
     */
    private Mono<String> parseAndUpload(String responseBody, Post post) {
        try {
            JsonNode data = objectMapper.readTree(responseBody).path("data");
            if (!data.isArray() || data.isEmpty()) {
                log.error("图片接口响应缺少 data 字段: {}", responseBody);
                return Mono.error(new IllegalStateException("API 响应未包含图片数据"));
            }
            JsonNode first = data.get(0);

            JsonNode urlNode = first.path("url");
            if (!urlNode.isMissingNode() && StringUtils.isNotBlank(urlNode.asText())) {
                String imageUrl = urlNode.asText();
                log.info("图片接口返回 URL: {}", imageUrl);
                return imageTransferService.updateFile(imageUrl, post);
            }

            JsonNode b64Node = first.path("b64_json");
            if (!b64Node.isMissingNode() && StringUtils.isNotBlank(b64Node.asText())) {
                log.info("图片接口返回 base64 数据，长度: {}", b64Node.asText().length());
                return uploadBase64(b64Node.asText(), post);
            }

            log.error("图片接口响应未包含 url / b64_json: {}", responseBody);
            return Mono.error(new IllegalStateException("API 响应未包含图片 URL 或 base64 数据"));
        } catch (Exception e) {
            log.error("解析图片接口响应失败: {}", e.getMessage());
            return Mono.error(new IllegalStateException("解析响应失败: " + e.getMessage()));
        }
    }

    /**
     * 上传 base64 图片，按 magic number 判定真实格式
     */
    private Mono<String> uploadBase64(String base64Data, Post post) {
        try {
            // 兼容 data:image/png;base64,xxx 形式
            String raw = base64Data.contains(",")
                ? base64Data.substring(base64Data.indexOf(',') + 1) : base64Data;
            byte[] imageBytes = Base64.getDecoder().decode(raw);
            MediaType mediaType = ImageUtils.detectMediaType(imageBytes);
            if (!ImageUtils.isImage(imageBytes)) {
                return Mono.error(new IllegalStateException("返回内容不是有效图片"));
            }
            String fileName = "cover-" + System.currentTimeMillis()
                + ImageUtils.detectExtension(imageBytes);
            DataBuffer buffer = new DefaultDataBufferFactory().wrap(imageBytes);
            Flux<DataBuffer> flux = Flux.just(buffer);
            log.info("上传 base64 封面图: {}, 类型: {}, 大小: {} B",
                fileName, mediaType, imageBytes.length);
            return imageTransferService.updateFile(flux, post, fileName, mediaType);
        } catch (Exception e) {
            log.error("base64 图片解码失败: {}", e.getMessage());
            return Mono.error(new IllegalStateException("base64 图片解码失败: " + e.getMessage()));
        }
    }
}
