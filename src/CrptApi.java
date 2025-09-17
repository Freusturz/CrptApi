import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * CrptApi — простой, расширяемый Java-клиент для работы с API "Честного знака".
 *
 * Требования:
 * - потокобезопасность;
 * - ограничение на количество запросов: N запросов за указанный интервал (timeUnit);
 * - блокирующее поведение при превышении лимита (потоки ждут, пока лимит освободится);
 * - однопайловая реализация; все дополнительные классы внутренние.
 */
public final class CrptApi {

    // Настройки клиента
    private final URI endpoint;
    private final String signatureHeader;
    private final HttpClient httpClient;
    private final Duration requestTimeout;

    // rate limiter
    private final long windowMillis;
    private final int requestLimit;
    // хранит моменты времени (ms) начала каждого запроса
    private final Deque<Long> timestamps = new ArrayDeque<>();

    /**
     * Создаёт экземпляр CrptApi с заданной конфигурацией.
     * Внешний код должен использовать Builder.
     */
    private CrptApi(Builder b) {
        this.endpoint = URI.create(Objects.requireNonNull(b.endpoint, "endpoint"));
        this.signatureHeader = Objects.requireNonNull(b.signatureHeader, "signatureHeader");
        this.requestTimeout = b.requestTimeout;
        this.httpClient = b.httpClient != null ? b.httpClient
                : HttpClient.newBuilder().connectTimeout(requestTimeout).build();
        this.windowMillis = b.timeUnit.toMillis(1); // интервал — 1 * timeUnit
        if (b.requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be positive");
        this.requestLimit = b.requestLimit;
    }

    /**
     * Блокирует текущий поток (если необходимо) до тех пор, пока не появится право на ещё один запрос.
     * Реализован как sliding window: храним времена предыдущих запросов; если записей >= limit,
     * ждём до момента, когда самая старая запись выйдет за предел окна.
     */
    private void acquirePermit() throws InterruptedException {
        synchronized (timestamps) {
            while (true) {
                long now = System.currentTimeMillis();
                // очистить просроченные метки
                while (!timestamps.isEmpty() && timestamps.peekFirst() + windowMillis <= now) {
                    timestamps.removeFirst();
                }
                if (timestamps.size() < requestLimit) {
                    // можно сделать запрос
                    timestamps.addLast(now);
                    return;
                } else {
                    // вычислить время ожидания до освобождения самой старой записи
                    long oldest = timestamps.peekFirst();
                    long waitMs = (oldest + windowMillis) - now;
                    if (waitMs <= 0) {
                        // цикл повторится и удалит просроченные
                        continue;
                    }
                    // Блокируемся на waitMs (без спин-лупа). Никаких notify() мы не делаем —
                    // здесь используется ожидание по таймауту, что корректно.
                    timestamps.wait(waitMs);
                }
            }
        }
    }

    /**
     * Метод для создания документа (ввод в оборот товара, произведённого в РФ).
     *
     * @param document  Java-объект документа (POJO, Map, List и т.д.)
     * @param signature Строка подписи (например, base64) — будет передана в заголовке
     * @return тело ответа API как строка
     * @throws IOException          при ошибках сети/ввода-вывода
     * @throws InterruptedException если текущий поток прерван (в т.ч. при ожидании лимита)
     */
    public String createDocument(Object document, String signature) throws IOException, InterruptedException {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(signature, "signature");

        // Ждём разрешения на запрос (rate limiting)
        acquirePermit();

        // сериализуем документ в JSON
        String json = JsonSerializer.serialize(document);

        // построим HTTP-запрос
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header(signatureHeader, signature)
                .POST(HttpRequest.BodyPublishers.ofString(json));

        HttpRequest request = reqBuilder.build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException ex) {
            // в случае исключения, не удаляем запись из timestamps:
            // timestamps представляют историю запросов, поэтому они остаются.
            throw ex;
        } finally {
            // опционально — можно уведомить ожидающие потоки, чтобы они могли пересчитать wait.
            // Это не строго необходимо: потоки ждут по таймауту, но notify ускорит пробуждение.
            synchronized (timestamps) {
                timestamps.notifyAll();
            }
        }

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return response.body();
        } else {
            throw new IOException("Unexpected response from CRPT API: " + status + " / body: " + response.body());
        }
    }

    /**
     * Builder для CrptApi — удобен для расширения конфигурации.
     */
    public static class Builder {
        private String endpoint;
        private String signatureHeader = "X-Signature";
        private TimeUnit timeUnit = TimeUnit.SECONDS;
        private int requestLimit = 1;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private HttpClient httpClient = null;

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder signatureHeader(String signatureHeader) {
            this.signatureHeader = signatureHeader;
            return this;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * Указывает максимально допустимое количество запросов за 1 timeUnit.
         */
        public Builder requestLimit(int requestLimit) {
            this.requestLimit = requestLimit;
            return this;
        }

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        public Builder httpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }

        public CrptApi build() {
            return new CrptApi(this);
        }
    }


    // Внутренний простой JSON-сериализатор (рефлексия, Map, List, примитивы)
    private static final class JsonSerializer {
        public static String serialize(Object obj) {
            try {
                StringBuilder sb = new StringBuilder();
                writeValue(sb, obj);
                return sb.toString();
            } catch (IllegalAccessException e) {
                throw new RuntimeException("JSON serialization error", e);
            }
        }

        private static void writeValue(StringBuilder sb, Object obj) throws IllegalAccessException {
            if (obj == null) {
                sb.append("null");
                return;
            }
            Class<?> cls = obj.getClass();

            if (obj instanceof String) {
                writeString(sb, (String) obj);
            } else if (obj instanceof Number || obj instanceof Boolean) {
                sb.append(obj.toString());
            } else if (obj instanceof Map) {
                writeMap(sb, (Map<?, ?>) obj);
            } else if (obj instanceof Iterable) {
                writeIterable(sb, (Iterable<?>) obj);
            } else if (cls.isArray()) {
                writeArray(sb, obj);
            } else if (cls.isEnum()) {
                writeString(sb, ((Enum<?>) obj).name());
            } else if (isPrimitiveWrapper(cls)) {
                sb.append(obj.toString());
            } else {
                writePojo(sb, obj);
            }
        }

        private static boolean isPrimitiveWrapper(Class<?> cls) {
            return cls == Integer.class || cls == Long.class || cls == Short.class || cls == Byte.class
                    || cls == Float.class || cls == Double.class || cls == Boolean.class || cls == Character.class;
        }

        private static void writeString(StringBuilder sb, String s) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '"':
                        sb.append("\\\"");
                        break;
                    case '\\':
                        sb.append("\\\\");
                        break;
                    case '\b':
                        sb.append("\\b");
                        break;
                    case '\f':
                        sb.append("\\f");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    default:
                        if (c < 0x20) {
                            sb.append(String.format("\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                }
            }
            sb.append('"');
        }

        private static void writeMap(StringBuilder sb, Map<?, ?> map) throws IllegalAccessException {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
        }

        private static void writeIterable(StringBuilder sb, Iterable<?> it) throws IllegalAccessException {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                writeValue(sb, o);
            }
            sb.append(']');
        }

        private static void writeArray(StringBuilder sb, Object array) throws IllegalAccessException {
            sb.append('[');
            int len = Array.getLength(array);
            for (int i = 0; i < len; i++) {
                if (i > 0) sb.append(',');
                Object val = Array.get(array, i);
                writeValue(sb, val);
            }
            sb.append(']');
        }

        private static void writePojo(StringBuilder sb, Object obj) throws IllegalAccessException {
            sb.append('{');
            boolean first = true;
            Class<?> cls = obj.getClass();
            // обходим поля, включая приватные. Игнорируем static и transient.
            for (Field field : getAllFields(cls)) {
                int mods = field.getModifiers();
                if (java.lang.reflect.Modifier.isStatic(mods)) continue;
                if (java.lang.reflect.Modifier.isTransient(mods)) continue;
                field.setAccessible(true);
                Object value = field.get(obj);
                if (!first) sb.append(',');
                first = false;
                writeString(sb, field.getName());
                sb.append(':');
                writeValue(sb, value);
            }
            sb.append('}');
        }

        private static List<Field> getAllFields(Class<?> cls) {
            List<Field> fields = new ArrayList<>();
            Class<?> cur = cls;
            while (cur != null && cur != Object.class) {
                for (Field f : cur.getDeclaredFields()) fields.add(f);
                cur = cur.getSuperclass();
            }
            return fields;
        }
    }

    // -----------------------------------------------------
    // Конец класса CrptApi
    // -----------------------------------------------------

    // Пример
    public static void main(String[] args) throws Exception {
        // Пример POJO-документа
        class Doc {
            public String id = "doc-1";
            public String product = "Автозапчасть";
            public int qty = 10;
        }

        // Создаём клиент: 3 запроса в 1 секунду
        CrptApi api = new CrptApi.Builder()
                .endpoint("https://postman-echo.com/post")
                .signatureHeader("X-Signature")
                .timeUnit(TimeUnit.SECONDS)
                .requestLimit(3)
                .requestTimeout(Duration.ofSeconds(15))
                .build();

        try {
            String response = api.createDocument(new Doc(), "MY_SIGNATURE");
            System.out.println("Response: " + response);
        } catch (IOException | InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}
