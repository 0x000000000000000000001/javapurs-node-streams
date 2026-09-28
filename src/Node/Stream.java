    // Port of Node/Stream.js. Streams carry their payload eagerly: a readable
    // exposes the bytes it holds, a writable appends to its target, and pipes
    // are listeners on the "data"/"end" events, which keeps the synchronous
    // effects of this backend observably ordered like the Node ones.
    public static class ReadableStream extends __M$Node_EventEmitter.EmitterBase {
        public byte[] data = new byte[0];
        public int position = 0;
        public boolean ended = false;
        public boolean destroyed = false;
        public Object error = null;
        public String encoding = null;
    }

    public static final class WritableStream extends ReadableStream {
        public String path = null;
        public boolean finished = false;
        public boolean corked = false;
        public String defaultEncoding = null;
    }

    private static void __flush(WritableStream writable) {
        if (writable.path == null) return;
        try {
            java.nio.file.Files.write(java.nio.file.Paths.get(writable.path), writable.data);
        } catch (java.io.IOException failure) {
            throw new RuntimeException(failure);
        }
    }

    private static void __finish(WritableStream writable) {
        if (writable.finished) return;
        if (writable.error != null) {
            writable.fire("error", writable.error);
            return;
        }
        writable.finished = true;
        __flush(writable);
        writable.fire("finish");
        writable.fire("end");
    }

    private static void __append(WritableStream writable, byte[] bytes) {
        byte[] joined = new byte[writable.data.length + bytes.length];
        System.arraycopy(writable.data, 0, joined, 0, writable.data.length);
        System.arraycopy(bytes, 0, joined, writable.data.length, bytes.length);
        writable.data = joined;
    }

    private static boolean __write(WritableStream writable, byte[] bytes) {
        if (writable.error != null) {
            writable.fire("error", writable.error);
            return false;
        }
        if (writable.finished) {
            writable.error = new RuntimeException("write after end");
            writable.fire("error", writable.error);
            return false;
        }
        __append(writable, bytes);
        writable.fire("data", writable.encoding == null
            ? __M$Node_Buffer.__wrap(bytes)
            : __M$Node_Buffer.__encode(bytes, writable.encoding));
        writable.fire("readable");
        return true;
    }

    private static void __callback(Object callback, Object error) {
        Object effect = ((java.util.function.Function<Object, Object>) callback).apply(error);
        ((java.util.function.Supplier<Object>) effect).get();
    }

    private static Object __chunk(ReadableStream readable, int limit) {
        if (readable.position >= readable.data.length) {
            if (!readable.ended) {
                readable.ended = true;
                readable.fire("end");
            }
            return null;
        }
        int available = readable.data.length - readable.position;
        int count = limit <= 0 ? available : Math.min(limit, available);
        byte[] chunk = java.util.Arrays.copyOfRange(readable.data, readable.position, readable.position + count);
        readable.position += count;
        return __M$Node_Buffer.__wrap(chunk);
    }

    private static void __pipe(ReadableStream readable, WritableStream writable) {
        byte[] pending = java.util.Arrays.copyOfRange(readable.data, readable.position, readable.data.length);
        readable.position = readable.data.length;
        if (pending.length > 0) __write(writable, pending);
        readable.listeners.computeIfAbsent("data", key -> new java.util.concurrent.CopyOnWriteArrayList<>())
            .add((java.util.function.Function<Object, Object>) chunk -> (java.util.function.Supplier<Object>) () -> {
                byte[] bytes = chunk instanceof __M$Node_Buffer.NodeBuffer
                    ? __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) chunk)
                    : __M$Node_Buffer.__decode((String) chunk, "utf8");
                __write(writable, bytes);
                return null;
            });
        readable.listeners.computeIfAbsent("end", key -> new java.util.concurrent.CopyOnWriteArrayList<>())
            .add((java.util.function.Function<Object, Object>) ignored -> (java.util.function.Supplier<Object>) () -> {
                __finish(writable);
                return null;
            });
    }

    public static Object readChunkImpl = (java.util.function.Function<Object, Object>) (left) ->
        (java.util.function.Function<Object, Object>) (right) ->
        (java.util.function.Function<Object, Object>) (chunk) ->
            (java.util.function.Supplier<Object>) () -> {
                Object effect = chunk instanceof __M$Node_Buffer.NodeBuffer
                    ? ((java.util.function.Function<Object, Object>) left).apply(chunk)
                    : ((java.util.function.Function<Object, Object>) right).apply(chunk);
                return ((java.util.function.Supplier<Object>) effect).get();
            };

    public static Object readImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Supplier<Object>) () -> __chunk((ReadableStream) readableObj, 0);

    public static Object readSizeImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Function<Object, Object>) (size) ->
            (java.util.function.Supplier<Object>) () -> __chunk((ReadableStream) readableObj, ((Number) size).intValue());

    public static Object setEncodingImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () -> {
                ((ReadableStream) readableObj).encoding = (String) encoding;
                return null;
            };

    public static Object readableImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) readableObj).position < ((ReadableStream) readableObj).data.length;

    public static Object readableEndedImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) readableObj).ended;

    public static Object readableFlowingImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Supplier<Object>) () -> !((ReadableStream) readableObj).ended;

    public static Object readableHighWaterMarkImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object readableLengthImpl = (java.util.function.Function<Object, Object>) (readableObj) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) readableObj).position < ((ReadableStream) readableObj).data.length;

    public static Object resumeImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Supplier<Object>) () -> null;

    public static Object pauseImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Supplier<Object>) () -> null;

    public static Object isPausedImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object pipeImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (writable) ->
            (java.util.function.Supplier<Object>) () -> { __pipe((ReadableStream) readable, (WritableStream) writable); return null; };

    public static Object pipeCbImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Function<Object, Object>) (options) ->
            (java.util.function.Supplier<Object>) () -> { __pipe((ReadableStream) readable, (WritableStream) writable); return null; };

    public static Object unpipeImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (writable) ->
            (java.util.function.Supplier<Object>) () -> null;

    public static Object unpipeAllImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Supplier<Object>) () -> null;

    public static Object writeableImpl = (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Supplier<Object>) () -> !((WritableStream) writable).finished;

    public static Object writeableEndedImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> ((WritableStream) writableObj).finished;

    public static Object writeableCorkedImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> ((WritableStream) writableObj).corked;

    public static Object erroredImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) stream).error != null;

    public static Object writeableFinishedImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> ((WritableStream) writableObj).finished;

    public static Object writeableHighWaterMarkImpl = (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Supplier<Object>) () -> 16384.0;

    public static Object writeableLengthImpl = (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Supplier<Object>) () -> 0.0;

    public static Object writeableNeedDrainImpl = (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object writeImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (bufferObj) ->
            (java.util.function.Supplier<Object>) () ->
                __write((WritableStream) writableObj, __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) bufferObj));

    public static Object writeCbImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (bufferObj) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                WritableStream writable = (WritableStream) writableObj;
                __write(writable, __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) bufferObj));
                __callback(callback, writable.error);
                return null;
            };

    public static Object writeStringImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (string) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () ->
                __write((WritableStream) writableObj, __M$Node_Buffer.__decode((String) string, (String) encoding));

    public static Object writeStringCbImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (string) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                WritableStream writable = (WritableStream) writableObj;
                __write(writable, __M$Node_Buffer.__decode((String) string, (String) encoding));
                __callback(callback, writable.error);
                return null;
            };

    public static Object corkImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> { ((WritableStream) writableObj).corked = true; return null; };

    public static Object uncorkImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> { ((WritableStream) writableObj).corked = false; return null; };

    public static Object setDefaultEncodingImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () -> {
                ((WritableStream) writableObj).defaultEncoding = (String) encoding;
                return null;
            };

    public static Object endImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Supplier<Object>) () -> { __finish((WritableStream) writableObj); return null; };

    public static Object endCbImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                WritableStream writable = (WritableStream) writableObj;
                __finish(writable);
                __callback(callback, writable.error);
                return null;
            };

    public static Object destroyImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> {
            ((ReadableStream) stream).destroyed = true;
            return null;
        };

    public static Object destroyErrorImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Function<Object, Object>) (error) ->
            (java.util.function.Supplier<Object>) () -> {
                ReadableStream readable = (ReadableStream) stream;
                readable.destroyed = true;
                readable.error = error;
                readable.fire("error", error);
                return null;
            };

    public static Object closedImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) stream).destroyed;

    public static Object destroyedImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> ((ReadableStream) stream).destroyed;

    public static Object allowHalfOpenImpl = (java.util.function.Function<Object, Object>) (duplex) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object pipelineImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (duplexes) ->
        (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                __pipe((ReadableStream) readable, (WritableStream) writable);
                __callback(callback, null);
                return null;
            };

    public static Object readableFromStrImpl = (java.util.function.Function<Object, Object>) (string) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () -> {
                ReadableStream stream = new ReadableStream();
                stream.data = __M$Node_Buffer.__decode((String) string, (String) encoding);
                return stream;
            };

    public static Object readableFromBufImpl = (java.util.function.Function<Object, Object>) (bufferObj) ->
        (java.util.function.Supplier<Object>) () -> {
            ReadableStream stream = new ReadableStream();
            stream.data = __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) bufferObj);
            return stream;
        };

    public static Object newPassThrough = (java.util.function.Supplier<Object>) () -> new WritableStream();
