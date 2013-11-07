using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using Amazon.S3;
using Amazon.S3.Model;
using ProtoBuf;
using S3Sync.Properties;
using log4net;

namespace S3Sync
{
    class Program
    {
        private static AmazonS3 s3;
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));
        private static FileIndexer indexer;
        private static S3Indexer s3indexer;
        public static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info("Initializing and connecting to AWS...");

            s3 = AWSClientFactory.CreateAmazonS3Client(RegionEndpoint.USWest1);
            indexer = new FileIndexer("Files");
            indexer.Index();

            s3indexer = new S3Indexer(Settings.Default.BucketName, Settings.Default.FolderName, "S3Tmp", s3);
            s3indexer.Index();

            log.Info("Comparing local index and remote index.");

            var filesToUpload = (from filePair in indexer.FileIndex where !s3indexer.HashedFiles.ContainsKey(filePair.Key) || !s3indexer.HashedFiles[filePair.Key].SequenceEqual(filePair.Value) select filePair.Key).ToList();
            var filesToDelete = (from filePair in s3indexer.HashedFiles where !indexer.FileIndex.ContainsKey(filePair.Key) select filePair.Key).ToList();

            foreach(var fileDelete in filesToDelete)
            {
                log.Debug("Deleting file "+fileDelete);
                s3.DeleteObject(new DeleteObjectRequest()
                                    {
                                        BucketName = Settings.Default.BucketName,
                                        Key = Settings.Default.FolderName + "/" + fileDelete
                                    });
            }

            foreach(var fileUpload in filesToUpload)
            {
                log.Debug("Uploading file "+fileUpload);
                s3.PutObject(new PutObjectRequest()
                                 {
                                     BucketName = Settings.Default.BucketName,
                                     Key = Settings.Default.FolderName + "/" + fileUpload,
                                     AutoCloseStream = true,
                                     InputStream = new FileStream("Files/" + fileUpload, FileMode.Open)
                                 });
            }

            log.Info("Re-indexing files...");

            using (MemoryStream stream = new MemoryStream())
            {
                Serializer.Serialize(stream, indexer.FileIndex);
                stream.Position = 0;
                s3.PutObject(new PutObjectRequest()
                {
                    BucketName = Settings.Default.BucketName,
                    Key = Settings.Default.FolderName + "/" + "index.mhash",
                    InputStream = stream
                });
            }

            log.Info("Done!");

            Console.Read();
        }
    }
}