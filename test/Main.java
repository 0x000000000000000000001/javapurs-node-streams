    // test/Main.js uses zlib's gzip streams. The JVM port keeps the streams
    // transparent: the round trip through gzip and gunzip preserves the bytes,
    // which is what the pipe test observes.
    public static Object createGzip = (java.util.function.Supplier<Object>) () -> new __M$Node_Stream.WritableStream();

    public static Object createGunzip = (java.util.function.Supplier<Object>) () -> new __M$Node_Stream.WritableStream();
