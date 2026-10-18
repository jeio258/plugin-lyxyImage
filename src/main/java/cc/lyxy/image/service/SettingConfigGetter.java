package cc.lyxy.image.service;

import lombok.Data;
import reactor.core.publisher.Mono;

public interface SettingConfigGetter {
    Mono<BasicConfig> getBasicConfig();

    Mono<AIConfig> getAIConfig();

    @Data
    class BasicConfig {
        public static final String GROUP = "basic";
        private String fileGroup;
        private String filePolicy;
        private String randomType;
    }

    @Data
    class AIConfig {
        public static final String GROUP = "ai";
        /** 生成平台：当前仅支持 OpenAI 兼容格式 */
        private String aiProvider = "openai";
        /** OpenAI 兼容接口基础地址，需含版本段，例如 https://api.openai.com/v1 */
        private String apiBaseUrl = "https://api.openai.com/v1";
        private String apiKey;
        private String model = "gpt-image-1";
        private String promptTemplate =
            "为这篇博客文章生成一张精美的封面图片。文章标题：{title}。文章内容摘要：{content}";
    }
}
