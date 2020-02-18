package no.ssb.dapla.gcs.ext;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class GoogleHadoopFileSystemExt extends GoogleHadoopFileSystem {

    @Override
    public FileStatus getFileStatus(Path hadoopPath) throws IOException {
        // Only reset gcs fs if namespacehas changed
        this.setGcsFs(this.createGcsFs(getConf()));
        return super.getFileStatus(hadoopPath);
    }

}
