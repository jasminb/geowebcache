/**
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * <p>You should have received a copy of the GNU Lesser General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Jose Macchi / Geosolutions 2009
 */
package org.geowebcache.storage.blobstore.file;

import static org.geowebcache.storage.blobstore.file.FilePathUtils.filteredLayerName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LayerMetadataStore {
    private static Log log =
            LogFactory.getLog(org.geowebcache.storage.blobstore.file.LayerMetadataStore.class);

    public static final String PROPERTY_METADATA_MAX_RW_ATTEMPTS =
            "gwc.layermetadatastore.maxRWAttempts";

    public static final String PROPERTY_WAIT_AFTER_RENAME =
            "gwc.layermetadatastore.waitAfterRename";

    static final String METADATA_GZIP_EXTENSION = ".gz";

    private static final long FLUSH_INTERVAL = 5000L;
    private final String path;
    private final LoadingCache<String, LayerMetaData> metaCache;
    private final Queue<LayerMetaData> updateQueue = new ConcurrentLinkedQueue<>();
    private final Timer writeTimer;

    public LayerMetadataStore(String rootPath) {
        this.path = rootPath;
        this.metaCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
                        .maximumSize(Integer.MAX_VALUE)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .build(
                                new CacheLoader<String, LayerMetaData>() {
                                    @Override
                                    public LayerMetaData load(String layerName) throws Exception {
                                        Properties properties = loadLayerMetadata(layerName);

                                        Map<String, String> data = new ConcurrentHashMap<>();

                                        for (Object key : properties.keySet()) {
                                            data.put((String) key, (String) properties.get(key));
                                        }
                                        return new LayerMetaData(layerName, data);
                                    }
                                });
        this.writeTimer = new Timer(true);
        this.writeTimer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        commitUpdates();
                    }
                },
                FLUSH_INTERVAL,
                FLUSH_INTERVAL);

        // Flush meta to disk in case of JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::commitUpdates));
    }

    private void commitUpdates() {
        LayerMetaData metaEntry;

        // Same entry can be added more than once to the update queue
        Set<LayerMetaData> uniqueMetaEntries = new HashSet<>();

        while ((metaEntry = updateQueue.poll()) != null) {
            uniqueMetaEntries.add(metaEntry);
        }

        for (LayerMetaData metaData : uniqueMetaEntries) {
            while (true) {
                int modifications = metaData.getModifications();

                Properties properties = new Properties();
                properties.putAll(metaData.getData());

                // We are good, flush it
                if (metaData.resetModifications(modifications)) {
                    File metaFile =
                            new File(getLayerDirectory(metaData.getLayer()), getMetadataFilename());
                    try {
                        writeMetadataFile(properties, metaFile);
                    } catch (IOException e) {
                        metaData.addModification();
                        updateQueue.add(metaData);
                        log.warn("Failed to write layer meta, layer=" + metaData.getLayer(), e);
                    }
                    break;
                }
            }
        }
    }

    private LayerMetaData get(String layerName) {
        try {
            return metaCache.get(layerName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> getLayerMetadata(String layerName) throws IOException {
        return new HashMap<>(get(layerName).getData());
    }

    public String getEntry(final String layerName, final String key) throws IOException {
        String value = getLayerMetadata(layerName).get(key);
        return value == null ? value : urlDecUtf8(value);
    }

    /**
     * @throws IOException
     * @see org.geowebcache.storage.BlobStore#putLayerMetadata(java.lang.String, java.lang.String,
     *     java.lang.String)
     */
    public void putEntry(final String layerName, final String key, final String value)
            throws IOException {
        LayerMetaData layerMetaData = get(layerName);

        String encodedValue = URLEncoder.encode(value, "UTF-8");

        String oldValue = layerMetaData.getData().get(key);

        // Do nothing
        if (oldValue != null && oldValue.equals(encodedValue)) {
            return;
        }

        layerMetaData.getData().put(key, encodedValue);
        layerMetaData.addModification();
        updateQueue.add(layerMetaData);
    }

    private File getLayerDirectory(String layerName) {
        String prefix = path + File.separator + filteredLayerName(layerName);
        return new File(prefix);
    }

    private static String urlDecUtf8(String value) {
        try {
            value = URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return value;
    }

    /**
     * Writes a Metadatafile with metadata parameter content
     *
     * @param metadata
     * @return temporal file or null if failed
     * @throws IOException
     */
    private File writeMetadataFile(Properties metadata, File metadataFile) throws IOException {
        createParentIfNeeded(metadataFile);
        String comments = "auto generated file, do not edit by hand";
        try (Writer writer = compressingWriter(metadataFile)) {
            metadata.store(writer, comments);
        }
        return metadataFile;
    }

    private void createParentIfNeeded(File metadataFile) {
        File parentDir = metadataFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            if (!parentDir.exists())
                throw new IllegalStateException(
                        "Unable to create parent directory " + parentDir.getAbsolutePath());
        }
    }

    private Writer compressingWriter(File file) throws IOException {
        return new OutputStreamWriter(
                new GZIPOutputStream(new FileOutputStream(file)), StandardCharsets.UTF_8);
    }

    private Properties getUncompressedLayerMetadata(final File metadataFile) throws IOException {
        return loadLayerMetadata(metadataFile, this::open);
    }

    private Properties loadLayerMetadata(File metadataFile, Function<File, InputStream> isProvider)
            throws IOException {
        if (!metadataFile.exists()) {
            return new Properties();
        }
        try (InputStream in = isProvider.apply(metadataFile)) {
            Properties properties = new Properties();
            properties.load(in);
            return properties;
        }
    }

    private InputStream openCompressed(File file) {
        try {
            return new GZIPInputStream(open(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InputStream open(File file) {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Properties loadLayerMetadata(final String layerName) throws IOException {
        File metadataFile = resolveMetadataFile(layerName);

        if (metadataFile.getName().equals(getLegacyMetadataFilename())) {
            return getUncompressedLayerMetadata(metadataFile);
        } else {
            return loadLayerMetadata(metadataFile, this::openCompressed);
        }
    }

    private String getMetadataFilename() {
        return getLegacyMetadataFilename() + LayerMetadataStore.METADATA_GZIP_EXTENSION;
    }

    private String getLegacyMetadataFilename() {
        return "metadata.properties";
    }

    /**
     * Returns the File related to {@code metadata.properties.gz}, upgrading the legacy {@code
     * metadata.properties} if needed
     *
     * @param layerName
     * @return metadata file (compressed or not, depending if it's present uncompressed)
     */
    private File resolveMetadataFile(final String layerName) {
        File layerPath = getLayerDirectory(layerName);

        File metaFile = new File(layerPath, getMetadataFilename());

        if (metaFile.exists()) {
            return metaFile;
        } else {
            // Fallback to legacy meta in case modern file is not found
            return new File(layerPath, getLegacyMetadataFilename());
        }
    }

    private static class LayerMetaData {
        private final Map<String, String> data;
        private final String layer;
        private final AtomicInteger modifications = new AtomicInteger(0);

        public LayerMetaData(String layer, Map<String, String> data) {
            this.data = data;
            this.layer = layer;
        }

        public Map<String, String> getData() {
            return data;
        }

        public String getLayer() {
            return layer;
        }

        public boolean hasChanges(int expectedModifications) {
            return modifications.get() == expectedModifications;
        }

        public int getModifications() {
            return modifications.get();
        }

        public boolean resetModifications(int expectedModifications) {
            return modifications.compareAndSet(expectedModifications, 0);
        }

        public void addModification() {
            modifications.incrementAndGet();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LayerMetaData metaData = (LayerMetaData) o;
            return getLayer().equals(metaData.getLayer());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getLayer());
        }
    }
}
