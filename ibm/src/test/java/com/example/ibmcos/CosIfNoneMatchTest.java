package com.example.ibmcos;

import com.example.ibmcos.service.CosService;
import com.example.ibmcos.service.CosService.PutResult;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.AmazonS3Exception;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.example.ibmcos.service.CosService.MAX_ENBANGO;
import static com.example.ibmcos.service.CosService.appendEnbango;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CosIfNoneMatchTest {

    private static final String BUCKET = "test-bucket";
    private static final String BASE_KEY = "if-none-match-test/sample.txt";

    @Mock AmazonS3 mockS3;

    CosService cosService;
    Set<String> existingKeys;
    final List<String> created = new ArrayList<>();

    @BeforeEach
    void setUp() {
        existingKeys = new HashSet<>();
        cosService = new CosService(mockS3);

        when(mockS3.doesBucketExistV2(anyString())).thenReturn(true);

        // Simulates If-None-Match: * conditional put and unconditional put in memory.
        when(mockS3.putObject(any(PutObjectRequest.class))).thenAnswer(invocation -> {
            PutObjectRequest req = invocation.getArgument(0);
            String key = req.getKey();
            Map<String, String> headers = req.getCustomRequestHeaders();
            boolean conditional = headers != null && "*".equals(headers.get("If-None-Match"));

            if (conditional && existingKeys.contains(key)) {
                AmazonS3Exception ex = new AmazonS3Exception("PreconditionFailed");
                ex.setStatusCode(412);
                ex.setErrorCode("PreconditionFailed");
                throw ex;
            }
            existingKeys.add(key);
            PutObjectResult result = new PutObjectResult();
            result.setETag("etag-" + Integer.toHexString(key.hashCode()));
            return result;
        });

        doAnswer(invocation -> {
            existingKeys.remove((String) invocation.getArgument(1));
            return null;
        }).when(mockS3).deleteObject(anyString(), anyString());

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
        PutObjectResult result = cosService.putIfAbsent(BUCKET, BASE_KEY, stream(content), content.length);
        assertThat(result.getETag()).isNotBlank();
        System.out.printf("[PASS] uploaded to '%s', ETag=%s%n", BASE_KEY, result.getETag());
    }

    @Test
    @DisplayName("putIfAbsent returns 412 when object already exists")
    void putIfAbsent_conflict_throws412() {
        seed(BASE_KEY, "original");

        assertThatThrownBy(() -> {
            byte[] dup = bytes("duplicate");
            cosService.putIfAbsent(BUCKET, BASE_KEY, stream(dup), dup.length);
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
            PutResult result = cosService.putWithEnbango(BUCKET, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(BASE_KEY);
            System.out.printf("[PASS] no conflict → used original key '%s'%n", result.usedKey());
        }

        @Test
        @DisplayName("falls back to _1 when original key exists")
        void oneConflict_usesEnbango1() {
            seed(BASE_KEY, "occupied");
            String expected = appendEnbango(BASE_KEY, 1);

            byte[] content = bytes("v2");
            PutResult result = cosService.putWithEnbango(BUCKET, BASE_KEY, stream(content), content.length);

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
            PutResult result = cosService.putWithEnbango(BUCKET, BASE_KEY, stream(content), content.length);

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
            PutResult result = cosService.putWithEnbango(BUCKET, BASE_KEY, stream(content), content.length);

            assertThat(result.usedKey()).isEqualTo(expected);
            System.out.printf("[PASS] 3 conflicts → used '%s' (max enbango reached, still succeeded)%n", result.usedKey());
        }

        @Test
        @DisplayName("throws 412 when all candidates (original + _1 + _2 + _3) are exhausted")
        void allExhausted_throwsException() {
            seed(BASE_KEY, "v0");
            for (int i = 1; i <= MAX_ENBANGO; i++) {
                seed(appendEnbango(BASE_KEY, i), "v" + i);
            }

            assertThatThrownBy(() -> {
                byte[] content = bytes("overflow");
                cosService.putWithEnbango(BUCKET, BASE_KEY, stream(content), content.length);
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
    // helpers
    // =========================================================================

    private void seed(String key, String content) {
        byte[] b = bytes(content);
        cosService.put(BUCKET, key, stream(b), b.length);
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
            try { cosService.deleteObject(BUCKET, k); } catch (Exception ignored) {}
        });
    }

    private static byte[] bytes(String s)               { return s.getBytes(StandardCharsets.UTF_8); }
    private static ByteArrayInputStream stream(byte[] b) { return new ByteArrayInputStream(b); }
}
