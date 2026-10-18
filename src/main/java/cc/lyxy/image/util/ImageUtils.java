package cc.lyxy.image.util;

import java.util.Optional;
import cc.lyxy.image.constant.ImageConstants;
import org.springframework.http.MediaType;

/**
 * 图片工具类
 */
public final class ImageUtils {
    private ImageUtils() {
        throw new AssertionError("工具类不应被实例化");
    }

    /**
     * 从URL获取文件扩展名
     */
    public static String getFileExtension(String url) {
        return Optional.ofNullable(url)
            .map(u -> {
                int qIdx = u.indexOf('?');
                var path = (qIdx >= 0) ? u.substring(0, qIdx) : u;
                int dotIdx = path.lastIndexOf('.');
                return (dotIdx >= 0) ? path.substring(dotIdx) : null;
            })
            .orElse(ImageConstants.DEFAULT_EXTENSION);
    }

    /**
     * 从MediaType获取文件扩展名
     */
    public static String getFileExtensionFromMediaType(String mediaType) {
        return ImageConstants.MEDIA_TYPE_TO_EXTENSION.getOrDefault(mediaType, ImageConstants.DEFAULT_EXTENSION);
    }

    /**
     * 从URL获取MediaType
     */
    public static MediaType getMediaTypeFromUrl(String url) {
        var extension = getFileExtension(url).toLowerCase();
        return ImageConstants.EXTENSION_TO_MEDIA_TYPE.getOrDefault(extension, MediaType.IMAGE_JPEG);
    }

    /**
     * 判断字节内容是否为受支持的图片（按 magic number 校验，防止把 HTML/空内容当图片转存）
     */
    public static boolean isImage(byte[] data) {
        return detectMediaType(data) != MediaType.APPLICATION_OCTET_STREAM;
    }

    /**
     * 根据 magic number 识别图片的 MediaType
     */
    public static MediaType detectMediaType(byte[] data) {
        if (data == null || data.length < 12) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
        // JPEG: FF D8 FF
        if ((data[0] & 0xFF) == 0xFF && (data[1] & 0xFF) == 0xD8 && (data[2] & 0xFF) == 0xFF) {
            return MediaType.IMAGE_JPEG;
        }
        // PNG: 89 50 4E 47
        if ((data[0] & 0xFF) == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G') {
            return MediaType.IMAGE_PNG;
        }
        // GIF: GIF8
        if (data[0] == 'G' && data[1] == 'I' && data[2] == 'F' && data[3] == '8') {
            return MediaType.IMAGE_GIF;
        }
        // WEBP: RIFF....WEBP
        if (data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F'
            && data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P') {
            return MediaType.parseMediaType("image/webp");
        }
        // BMP: BM
        if (data[0] == 'B' && data[1] == 'M') {
            return MediaType.parseMediaType("image/bmp");
        }
        return MediaType.APPLICATION_OCTET_STREAM;
    }

    /**
     * 根据 magic number 推断文件扩展名
     */
    public static String detectExtension(byte[] data) {
        var mediaType = detectMediaType(data);
        if (MediaType.IMAGE_JPEG.equals(mediaType)) {
            return ".jpg";
        } else if (MediaType.IMAGE_PNG.equals(mediaType)) {
            return ".png";
        } else if (MediaType.IMAGE_GIF.equals(mediaType)) {
            return ".gif";
        } else if ("image/webp".equals(mediaType.toString())) {
            return ".webp";
        } else if ("image/bmp".equals(mediaType.toString())) {
            return ".bmp";
        }
        return ImageConstants.DEFAULT_EXTENSION;
    }

    /**
     * 取 URL 的 origin 作为 Referer，绕开部分图床的防盗链
     */
    public static String refererOf(String url) {
        try {
            var uri = java.net.URI.create(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return "";
            }
            var port = uri.getPort() > 0 ? ":" + uri.getPort() : "";
            return uri.getScheme() + "://" + uri.getHost() + port + "/";
        } catch (Exception e) {
            return "";
        }
    }
} 