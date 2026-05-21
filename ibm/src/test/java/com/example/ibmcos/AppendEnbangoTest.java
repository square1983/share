package com.example.ibmcos;

import org.junit.jupiter.api.Test;

import static com.example.ibmcos.service.CosService.appendEnbango;
import static org.assertj.core.api.Assertions.assertThat;

class AppendEnbangoTest {

    @Test void withExtension() { assertThat(appendEnbango("dir/file.txt", 1)).isEqualTo("dir/file_1.txt"); }
    @Test void noExtension()   { assertThat(appendEnbango("noext", 2)).isEqualTo("noext_2"); }
    @Test void hiddenFile()    { assertThat(appendEnbango(".hidden", 1)).isEqualTo(".hidden_1"); }
    @Test void multiDot()      { assertThat(appendEnbango("a.b.c", 3)).isEqualTo("a.b_3.c"); }
}
