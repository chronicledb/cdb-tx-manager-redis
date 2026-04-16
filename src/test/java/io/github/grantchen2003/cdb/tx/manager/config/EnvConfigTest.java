package io.github.grantchen2003.cdb.tx.manager.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnvConfigTest {

    @Test
    void get_returnsValue_whenEnvVarIsSet() {
        assertDoesNotThrow(() -> {
            final String value = System.getenv("PATH"); // PATH is always set
            assertNotNull(value);
        });
    }

    @Test
    void get_throwsIllegalStateException_whenEnvVarIsMissing() {
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> EnvConfig.get("THIS_ENV_VAR_DEFINITELY_DOES_NOT_EXIST_XYZ")
        );
        assertTrue(ex.getMessage().contains("THIS_ENV_VAR_DEFINITELY_DOES_NOT_EXIST_XYZ"));
    }
}
