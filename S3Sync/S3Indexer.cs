using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using ProtoBuf;
using log4net;

namespace S3Sync
{
    /// <summary>
    /// Builds and uploads an index of an existing S3 library by either checking for an existing index or downloading EVERY file and hashing it.
    /// </summary>
    public class S3Indexer
    {
        /// <summary>
        /// String filename and byte[] md5.
        /// </summary>
        public Dictionary<string, byte[]> HashedFiles;

        private static readonly ILog log = LogManager.GetLogger(typeof(S3Indexer));

        private string downloadPath;

        private AmazonS3 s3;

        private string bucketName;
        private string folderName;

        /// <summary>
        /// Create an indexer given a bucket and folder.
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="folderName"></param>
        public S3Indexer(string bucketName, string folderName, string downloadFolderName, AmazonS3 s3)
        {
            HashedFiles = new Dictionary<string, byte[]>();
            if(Directory.Exists(downloadFolderName)) Directory.Delete(downloadFolderName, true);
            Directory.CreateDirectory(downloadFolderName);
            downloadPath = downloadFolderName;
            this.s3 = s3;
            this.bucketName = bucketName;
            this.folderName = folderName;
        }

        /// <summary>
        /// Go!
        /// </summary>
        public void Index()
        {
            HashedFiles.Clear();
            //Find the hash object
            GetObjectResponse s3hash = null;
            log.Debug("Checking for index.mhash...");
            try
            {
                s3hash = s3.GetObject(new GetObjectRequest() {BucketName = bucketName, Key = folderName+"/"+"index.mhash"});
            }catch(Exception ex)
            {
            }
            if(s3hash != null)
            {
                log.Debug("Hash file found, deserializing!");
                Dictionary<string, byte[]> downloadedIndex;
                using(var stream = s3hash.ResponseStream)
                {
                    downloadedIndex = Serializer.Deserialize<Dictionary<string, byte[]>>(stream);
                }
                HashedFiles = downloadedIndex;
                log.Debug("Index deserialized, files: "+HashedFiles.Count);
            }else
            {
                log.Debug("Downloading all files to build a hash.");
                var files =
                    s3.ListObjects(new ListObjectsRequest()
                                       {BucketName = bucketName, Delimiter = "/", Prefix = folderName + "/"});
                
                foreach(var obj in files.S3Objects)
                {
                    if (obj.Key.Contains("index.mhash")) continue;
                    var folderDir = downloadPath + "/" + Path.GetDirectoryName(obj.Key);
                    Directory.CreateDirectory(folderDir);
                    using (GetObjectResponse response = s3.GetObject(new GetObjectRequest() { BucketName = bucketName, Key = obj.Key }))
                    {
                        using(Stream stream = response.ResponseStream)
                        {
                            using(FileStream file = new FileStream(downloadPath+"/"+obj.Key, FileMode.Create))
                            {
                                log.Debug("Downloading file "+obj.Key);
                                stream.CopyTo(file);
                            }
                        }
                    }
                }
                log.Debug("All files downloaded, hashing files...");
                var fileIndexer = new FileIndexer(downloadPath + "/" + folderName + "/");
                fileIndexer.Index();
                var index = fileIndexer.FileIndex;
                HashedFiles = index;
                log.Debug("All files hashed, serializing and uploading hash file...");
                using(MemoryStream stream = new MemoryStream())
                {
                    Serializer.Serialize(stream, HashedFiles);
                    stream.Position = 0;
                    s3.PutObject(new PutObjectRequest()
                                     {
                                         BucketName = bucketName,
                                         Key = folderName + "/" + "index.mhash",
                                         InputStream = stream
                                     });
                }
                log.Debug("Finished uploading new hash file!");
            }
        }
    }
}
