package group.gnometrading.migration;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

/** The few S3 and zstd operations the migration uses. */
final class S3Files {

    private static final int COMPRESSION_LEVEL = 3;
    private static final int DELETE_BATCH = 1000;

    private final S3Client s3;

    S3Files(final S3Client s3) {
        this.s3 = s3;
    }

    List<String> keys(final String bucket) {
        return this.s3.listObjectsV2Paginator(r -> r.bucket(bucket)).contents().stream()
                .map(S3Object::key)
                .toList();
    }

    byte[] get(final String bucket, final String key) {
        return this.s3.getObjectAsBytes(r -> r.bucket(bucket).key(key)).asByteArray();
    }

    /** The object's bytes, or null if there is none. */
    byte[] getIfExists(final String bucket, final String key) {
        try {
            return get(bucket, key);
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    void put(final String bucket, final String key, final byte[] data) {
        this.s3.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromBytes(data));
    }

    void delete(final String bucket, final List<String> keys) {
        for (int from = 0; from < keys.size(); from += DELETE_BATCH) {
            final List<ObjectIdentifier> batch = keys.subList(from, Math.min(keys.size(), from + DELETE_BATCH)).stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .toList();
            this.s3.deleteObjects(
                    r -> r.bucket(bucket).delete(d -> d.objects(batch).quiet(true)));
        }
    }

    static byte[] decompress(final byte[] compressed) {
        try (ZstdInputStream in = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static byte[] compress(final byte[] data) {
        return Zstd.compress(data, COMPRESSION_LEVEL);
    }
}
