    // Port of Node/Stream.js. Streams carry their payload eagerly: a readable
    // holds the bytes it would emit, a writable writes them to its target when
    // it ends, which matches how the synchronous effects are observed.
    public static final class ReadableStream extends __M$Node_EventEmitter.EmitterBase {
        public byte[] data = new byte[0];
        public int position = 0;
        public boolean ended = false;
        public boolean destroyed = false;
    }

    public static final class WritableStream extends __M$Node_EventEmitter.EmitterBase {
        public String path = null;
        public byte[] data = new byte[0];
        public boolean finished = false;
        public boolean corked = false;
        public boolean destroyed = false;
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
        writable.finished = true;
        __flush(writable);
        writable.fire("finish");
    }

    private static void __append(WritableStream writable, byte[] bytes) {
        byte[] joined = new byte[writable.data.length + bytes.length];
        System.arraycopy(writable.data, 0, joined, 0, writable.data.length);
        System.arraycopy(bytes, 0, joined, writable.data.length, bytes.length);
        writable.data = joined;
    }

    private static Object __chunk(ReadableStream readable, int limit) {
        if (readable.position >= readable.data.length) {
            readable.ended = true;
            readable.fire("end");
            return null;
        }
        int count = Math.min(limit <= 0 ? readable.data.length - readable.position : limit, readable.data.length - readable.position);
        byte[] chunk = java.util.Arrays.copyOfRange(readable.data, readable.position, readable.position + count);
        readable.position += count;
        return __M$Node_Buffer.__wrap(chunk);
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

    public static Object setEncodingImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () -> null;

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

    private static void __pipe(Object readableObj, Object writableObj) {
        ReadableStream readable = (ReadableStream) readableObj;
        WritableStream writable = (WritableStream) writableObj;
        __append(writable, java.util.Arrays.copyOfRange(readable.data, readable.position, readable.data.length));
        readable.position = readable.data.length;
        readable.ended = true;
        __finish(writable);
        writable.fire("finish");
        readable.fire("end");
    }

    public static Object pipeImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (writable) ->
            (java.util.function.Supplier<Object>) () -> { __pipe(readable, writable); return null; };

    public static Object pipeCbImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Function<Object, Object>) (options) ->
            (java.util.function.Supplier<Object>) () -> { __pipe(readable, writable); return null; };

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
        (java.util.function.Supplier<Object>) () -> false;

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
            (java.util.function.Supplier<Object>) () -> {
                WritableStream writable = (WritableStream) writableObj;
                __append(writable, __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) bufferObj));
                return true;
            };

    public static Object writeCbImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (bufferObj) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                WritableStream writable = (WritableStream) writableObj;
                __append(writable, __M$Node_Buffer.__window((__M$Node_Buffer.NodeBuffer) bufferObj));
                Object effect = ((java.util.function.Function<Object, Object>) callback).apply(null);
                ((java.util.function.Supplier<Object>) effect).get();
                return true;
            };

    public static Object writeStringImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (string) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
            (java.util.function.Supplier<Object>) () -> {
                __append((WritableStream) writableObj, __M$Node_Buffer.__decode((String) string, (String) encoding));
                return true;
            };

    public static Object writeStringCbImpl = (java.util.function.Function<Object, Object>) (writableObj) ->
        (java.util.function.Function<Object, Object>) (string) ->
        (java.util.function.Function<Object, Object>) (encoding) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                __append((WritableStream) writableObj, __M$Node_Buffer.__decode((String) string, (String) encoding));
                Object effect = ((java.util.function.Function<Object, Object>) callback).apply(null);
                ((java.util.function.Supplier<Object>) effect).get();
                return true;
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
                __finish((WritableStream) writableObj);
                Object effect = ((java.util.function.Function<Object, Object>) callback).apply(null);
                ((java.util.function.Supplier<Object>) effect).get();
                return null;
            };

    public static Object destroyImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> {
            if (stream instanceof WritableStream) ((WritableStream) stream).destroyed = true;
            else if (stream instanceof ReadableStream) ((ReadableStream) stream).destroyed = true;
            return null;
        };

    public static Object destroyErrorImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Function<Object, Object>) (error) ->
            (java.util.function.Supplier<Object>) () -> null;

    public static Object closedImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object destroyedImpl = (java.util.function.Function<Object, Object>) (stream) ->
        (java.util.function.Supplier<Object>) () ->
            stream instanceof WritableStream
                ? ((WritableStream) stream).destroyed
                : ((ReadableStream) stream).destroyed;

    public static Object allowHalfOpenImpl = (java.util.function.Function<Object, Object>) (duplex) ->
        (java.util.function.Supplier<Object>) () -> false;

    public static Object pipelineImpl = (java.util.function.Function<Object, Object>) (readable) ->
        (java.util.function.Function<Object, Object>) (duplexes) ->
        (java.util.function.Function<Object, Object>) (writable) ->
        (java.util.function.Function<Object, Object>) (callback) ->
            (java.util.function.Supplier<Object>) () -> {
                __pipe(readable, writable);
                Object effect = ((java.util.function.Function<Object, Object>) callback).apply(null);
                ((java.util.function.Supplier<Object>) effect).get();
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
