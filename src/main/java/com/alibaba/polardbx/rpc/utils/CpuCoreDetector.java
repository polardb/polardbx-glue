package com.alibaba.polardbx.rpc.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;

/**
 * This class derives from <a href="https://github.com/wildfly/wildfly-common/blob/master/src/main/java/org/wildfly/common/cpu/ProcessorInfo.java">wildfly-common</a>
 * licensed under the Apache Software License 2.0.
 */
public final class CpuCoreDetector {
    private CpuCoreDetector() {
    }

    private static final String PROC_FILE = "/proc/self/status";
    private static final String CPUS_ALLOWED_PREFIX = "Cpus_allowed:";
    private static final byte[] BITS = new byte[]{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

    /**
     * Returns the number of processors available to this process. On most operating systems this method
     * simply delegates to {@link Runtime#availableProcessors()}. However, on Linux, this strategy
     * is insufficient, since the JVM does not take into consideration the process' CPU set affinity
     * which is employed by cgroups and numactl. Therefore this method will analyze the Linux proc filesystem
     * to make the determination. Since the CPU affinity of a process can be change at any time, this method does
     * not cache the result. Calls should be limited accordingly.
     * <br>
     * Note tha on Linux, both SMT units (Hyper-Threading) and CPU cores are counted as a processor.
     *
     * @return the available processors on this system.
     */
    public static int availableProcessors() {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged((PrivilegedAction<Integer>) CpuCoreDetector::determineProcessors);
        }

        return determineProcessors();
    }

    private static int determineProcessors() {
        int javaProcs = Runtime.getRuntime().availableProcessors();
        if (!isLinux()) {
            return javaProcs;
        }

        int maskProcs = 0;

        try {
            maskProcs = readCPUMask();
        } catch (Exception e) {
            // skip
        }

        return maskProcs > 0 ? Math.min(javaProcs, maskProcs) : javaProcs;
    }

    private static int readCPUMask() throws IOException {
        try (final FileInputStream stream = new FileInputStream(PROC_FILE);
             final InputStreamReader inputReader = new InputStreamReader(stream, StandardCharsets.US_ASCII);
             final BufferedReader reader = new BufferedReader(inputReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(CPUS_ALLOWED_PREFIX)) {
                    int count = 0;
                    int start = CPUS_ALLOWED_PREFIX.length();
                    for (int i = start; i < line.length(); i++) {
                        char ch = line.charAt(i);
                        if (ch >= '0' && ch <= '9') {
                            count += BITS[ch - '0'];
                        } else if (ch >= 'a' && ch <= 'f') {
                            count += BITS[ch - 'a' + 10];
                        } else if (ch >= 'A' && ch <= 'F') {
                            count += BITS[ch - 'A' + 10];
                        }
                    }
                    return count;
                }
            }
        }

        return -1;
    }

    private static boolean isLinux() {
        String osArch = System.getProperty("os.name", "unknown").toLowerCase(Locale.US);
        return (osArch.contains("linux"));
    }
}
