package com.example.ibmcos;

import com.example.ibmcos.service.CosService;
import com.example.ibmcos.service.CosService.PutResult;
import com.ibm.cloud.objectstorage.services.s3.model.AmazonS3Exception;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.example.ibmcos.service.CosService.MAX_ENBANGO;
import static com.example.ibmcos.service.CosService.appendEnbango;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class CosIfNoneMatchTest {

    private static final String BASE_KEY = "if-none-match-test/sample.txt";

    @Autowired CosService cosService;
    @Value("${cos.test-bucket}") String bucket;

    // keys created during a test — cleaned up in tearDown
    final List<String> created = new ArrayList<>();

    @BeforeEach
    void setUp() {
        if (!cosService.bucketExists(bucket)) {
            cosService.createBucket(bucket);
        }
        deleteQuietly(allCandidates(BASE_KEY));
    }

    @AfterEach
    void tearDown() {
        deleteQuietly(allCandidates(BASE_KEY));
        deleteQuietly(created);
        created.clear();
    }

    // =========================================================================
    // putIfAbsent — original behaviour
    // =========================================================================

    @Test
    @DisplayName("putIfAbsent succeeds when object does not exist")
    void putIfAbsent_noConflict_succeeds() {
        byte[] content = bytes("hello");
        PutObjectResult result = cosService.putIfAbsent(bucket, BASE_KEY, stream(content), content.length);
        assertThat(result.getETag()).isNotBlank();
        System.out.printf("[PASS] uploaded to '%s', ETag=%s%n", BASE_KEY, result.getETag());
    }

    @Test
    @DisplayName("putIfAbsent returns 412 when object already exists")
    void putIfAbsent_conflict_throws412() {
        seed(BASE_KEY, "original");

        assertThatThrownBy(() -> {
            byte[] dup = bytes("duplicate");
            cosService.putIfAbsent(bucket, BASE_KEY, stream(dup), dup.length);
        })
                .isInstanceOf(AmazonS3Exception.class)
                .satisfies(ex -> {
                    int status = ((AmazonS3Exception) ex).getStatusCode();
                    System.out.printf("[result] HTTP %d %s%n", status, ((AmazonS3Exception) ex).getErrorCode());
                    assertThat(status).isEqualTo(412);
                });
        System.out.println("[PASS] Conflict correctly rejected with 412.");
    }

    // =========================================================================
    // putWithEnbango — 枝番 fallback logic
    // =========================================================================

    @Nested
    @DisplayName("putWithEnbango")
    class PutWithEnbangoTests {

        @Test
        @DisplayName("uses original key when no conflict")
        void noConflict_usesOriginalKey() {
            byte[] content = bytes("v1");
            PutResult result = cosService.putWithEnbango(bucket, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(BASE_KEY);
            System.out.printf("[PASS] no conflict → used original key '%s'%n", result.usedKey());
        }

        @Test
        @DisplayName("falls back to _1 when original key exists")
        void oneConflict_usesEnbango1() {
            seed(BASE_KEY, "occupied");
            String expected = appendEnbango(BASE_KEY, 1);

            byte[] content = bytes("v2");
            PutResult result = cosService.putWithEnbango(bucket, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(expected);
            System.out.printf("[PASS] 1 conflict → used '%s'%n", result.usedKey());
        }

        @Test
        @DisplayName("falls back to _2 when original and _1 both exist")
        void twoConflicts_usesEnbango2() {
            seed(BASE_KEY, "v1");
            seed(appendEnbango(BASE_KEY, 1), "v1_1");
            String expected = appendEnbango(BASE_KEY, 2);

            byte[] content = bytes("v2");
            PutResult result = cosService.putWithEnbango(bucket, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(expected);
            System.out.printf("[PASS] 2 conflicts → used '%s'%n", result.usedKey());
        }

        @Test
        @DisplayName("falls back to _3 when original, _1, and _2 all exist")
        void threeConflicts_usesEnbango3() {
            seed(BASE_KEY, "v1");
            seed(appendEnbango(BASE_KEY, 1), "v1_1");
            seed(appendEnbango(BASE_KEY, 2), "v1_2");
            String expected = appendEnbango(BASE_KEY, 3);

            byte[] content = bytes("v2");
            PutResult result = cosService.putWithEnbango(bucket, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(expected);
            System.out.printf("[PASS] 3 conflicts → used '%s' (max enbango reached, still succeeded)%n", result.usedKey());
        }

        @Test
        @DisplayName("throws 412 when all candidates (original + _1 + _2 + _3) are exhausted")
        void allExhausted_throwsException() {
            // pre-occupy original + all 3 enbango slots
            seed(BASE_KEY, "v0");
            for (int i = 1; i <= MAX_ENBANGO; i++) {
                seed(appendEnbango(BASE_KEY, i), "v" + i);
            }

            assertThatThrownBy(() -> {
                byte[] content = bytes("overflow");
                cosService.putWithEnbango(bucket, BASE_KEY, stream(content), content.length);
            })
                    .isInstanceOf(AmazonS3Exception.class)
                    .satisfies(ex -> {
                        int status = ((AmazonS3Exception) ex).getStatusCode();
                        System.out.printf("[result] all slots occupied → threw HTTP %d as expected%n", status);
                        assertThat(status).isEqualTo(412);
                    });
            System.out.println("[PASS] All enbango exhausted — exception thrown correctly.");
        }
    }

    // =========================================================================
    // appendEnbango unit tests (no network)
    // =========================================================================

    @Nested
    @DisplayName("appendEnbango (unit)")
    class AppendEnbangoTests {

        @Test void withExtension()  { assertThat(appendEnbango("dir/file.txt", 1)).isEqualTo("dir/file_1.txt"); }
        @Test void noExtension()    { assertThat(appendEnbango("noext", 2)).isEqualTo("noext_2"); }
        @Test void hiddenFile()     { assertThat(appendEnbango(".hidden", 1)).isEqualTo(".hidden_1"); }
        @Test void multiDot()       { assertThat(appendEnbango("a.b.c", 3)).isEqualTo("a.b_3.c"); }
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private void seed(String key, String content) {
        byte[] b = bytes(content);
        cosService.put(bucket, key, stream(b), b.length);
        created.add(key);
    }

    private List<String> allCandidates(String key) {
        List<String> keys = new ArrayList<>();
        keys.add(key);
        for (int i = 1; i <= MAX_ENBANGO; i++) {
            keys.add(appendEnbango(key, i));
        }
        return keys;
    }

    private void deleteQuietly(List<String> keys) {
        keys.forEach(k -> {
            try { cosService.deleteObject(bucket, k); } catch (Exception ignored) {}
        });
    }

    private static byte[] bytes(String s)          { return s.getBytes(StandardCharsets.UTF_8); }
    private static ByteArrayInputStream stream(byte[] b) { return new ByteArrayInputStream(b); }
}
