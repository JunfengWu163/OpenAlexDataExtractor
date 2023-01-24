using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Catalyst.Models;
using Mosaik.Core;
using Catalyst;
using Newtonsoft.Json;

namespace OpenAlexDataExtractor
{
    public class Work
    {
        [BsonElement("work_id")]
        public string id { get; set; }
        [BsonElement("year")]
        public UInt16 publication_year { get; set; }
        [BsonElement("title")]
        public string title { get; set; }
        [BsonElement("venue")]
        public Venue host_venue { get; set; }
        [BsonElement("references")]
        public List<string> referenced_works { get; set; }
        [BsonElement("concepts")]
        public List<XConcept> concepts { get; set; }
        [BsonElement("authorships")]
        public List<Authorship> authorships { get; set; }

        UInt64 GetIdNumber()
        {
            return GetIdNumber(id);
        }

        UInt64 GetIdNumber(string idStr)
        {
            string[] idParts = idStr.Split('/');
            UInt64 idNumber = Convert.ToUInt64(idParts[idParts.Length - 1].Substring(1));
            return idNumber;
        }

        Byte[] GetTitleBytes()
        {
            return Encoding.UTF8.GetBytes(title);
        }

        public void Deserialize(BinaryReader dataReader, UInt64 idNumber, Int64 workPos)
        {

        }

        public void Serialize(Int64 workPos, BinaryWriter dataWriter, BinaryWriter indexWriter, ConcurrentDictionary<UInt64,string> authors)
        {
            Byte[] titleBytes = GetTitleBytes();
            UInt64 idNumber = GetIdNumber();
            UInt64 idBucket = idNumber & 0xff;
            UInt64 venueIdNumber = host_venue != null ? host_venue.GetIdNumber() : 0;
            UInt16 titleLength = Convert.ToUInt16(titleBytes.Length);
            UInt16 numConcepts = Convert.ToUInt16(concepts != null ? concepts.Count : 0);
            UInt16 numReferences = Convert.ToUInt16(referenced_works != null ? referenced_works.Count : 0);
            UInt16 numAuthorships = Convert.ToUInt16(authorships != null ? authorships.Count : 0);
            
            indexWriter.Write(idNumber);
            indexWriter.Write(workPos);

            dataWriter.Write(idNumber);
            dataWriter.Write(venueIdNumber);
            dataWriter.Write(publication_year);
            dataWriter.Write(titleLength);
            dataWriter.Write(numConcepts);
            dataWriter.Write(numReferences);
            dataWriter.Write(numAuthorships);
            dataWriter.Write(titleBytes);
            for (UInt16 idxConcept = 0; idxConcept < numConcepts; idxConcept++)
            {
                UInt64 conceptIdNumber = concepts[idxConcept].GetIdNumber();
                double score = concepts[idxConcept].score;
                dataWriter.Write(conceptIdNumber);
                dataWriter.Write(score);
            }
            for (UInt16 idxReference = 0; idxReference < numReferences; idxReference++)
            {
                UInt64 referenceIdNumber = GetIdNumber(referenced_works[idxReference]);
                dataWriter.Write(referenceIdNumber);
            }
            for (UInt16 idxAuthorship = 0; idxAuthorship < numAuthorships; idxAuthorship++)
            {
                UInt64 authorIdNumber = authorships[idxAuthorship].author.GetIdNumber();
                UInt16 positionNumber = authorships[idxAuthorship].GetPositionNumber();
                dataWriter.Write(authorIdNumber);
                dataWriter.Write(positionNumber);
                authorships[idxAuthorship].author.UpdateAuthors(authors);
            }

        }

        public static async void ExtractUnsorted(string inputPath, string outputPath, int numThreads)
        {
            ConcurrentDictionary<UInt64, string> authors = new ConcurrentDictionary<ulong, string>();

            Storage.Current = new DiskStorage("catalyst-models");
            var cld2LanguageDetector = await LanguageDetector.FromStoreAsync(Language.Any, Mosaik.Core.Version.Latest, "");

            string worksPath = Path.Combine(inputPath, "works");
            DirectoryInfo diWorksPath = new DirectoryInfo(worksPath);
            foreach (var workDir in diWorksPath.GetDirectories())
            {
                Parallel.ForEach(workDir.GetFiles(), new ParallelOptions { MaxDegreeOfParallelism = numThreads }, workFile =>
                {
                    int threadId = Thread.CurrentThread.ManagedThreadId;

                    if (workFile.Extension == ".gz")
                    {
                        Console.WriteLine(workFile.FullName);

                        using (FileStream workDataFile = new FileStream(Path.Combine(outputPath, $"work-data-{threadId}.wjf"), FileMode.Append))
                        {
                            using (BinaryWriter workDataWriter = new BinaryWriter(workDataFile))
                            {
                                using (FileStream workIndexFile = new FileStream(Path.Combine(outputPath, $"work-index-{threadId}.wjf"), FileMode.Append))
                                {
                                    using (BinaryWriter workIndexWriter = new BinaryWriter(workIndexFile))
                                    {
                                        using (FileStream gzFile = File.OpenRead(workFile.FullName))
                                        {
                                            using (GZipStream gzStream = new GZipStream(gzFile, CompressionMode.Decompress))
                                            {
                                                using (StreamReader reader = new StreamReader(gzStream))
                                                {
                                                    string? line = reader.ReadLine();
                                                    while (line != null)
                                                    {
                                                        if (line.Contains("publication_year"))
                                                        {
                                                            try
                                                            {
                                                                OpenAlexDataExtractor.Work work = JsonConvert.DeserializeObject<OpenAlexDataExtractor.Work>(line);
                                                                var doc = new Document(work.title);
                                                                cld2LanguageDetector.Process(doc);
                                                                if (doc.Language == Language.English)
                                                                {
                                                                    work.Serialize(workDataFile.Position, workDataWriter, workIndexWriter, authors);
                                                                }
                                                            }
                                                            catch (Exception e)
                                                            {
                                                                Console.WriteLine(e.Message);
                                                            }
                                                        }

                                                        line = reader.ReadLine();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }

            OpenAlexDataExtractor.Author.Serialize(authors, outputPath, numThreads);
        }
    }
}
