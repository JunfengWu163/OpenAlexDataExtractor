using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;

namespace OpenAlexDataExtractor
{
    [BsonIgnoreExtraElements]
    public class Concept
    {
        [BsonElement("concept_id")]
        public string id { get; set; }

        [BsonElement("name")]
        public string display_name { get; set; }

        [BsonElement("level")]
        public UInt16 level { get; set; }

        [BsonElement("ancestors")]
        public List<Concept> ancestors { get; set; }

        public UInt64 GetIdNumber()
        {
            return GetIdNumber(id);
        }

        UInt64 GetIdNumber(string idStr)
        {
            if (idStr == null) return 0;
            string[] idParts = idStr.Split('/');
            UInt64 idNumber = Convert.ToUInt64(idParts[idParts.Length - 1].Substring(1));
            return idNumber;
        }

        Byte[] GetNameBytes()
        {
            return Encoding.UTF8.GetBytes(display_name);
        }

        public void Serialize(Int64 conceptPos, BinaryWriter dataWriter, BinaryWriter indexWriter)
        {
            indexWriter.Write(GetIdNumber());
            indexWriter.Write(conceptPos);

            Byte[] nameBytes = GetNameBytes();
            UInt16 nameLength = Convert.ToUInt16(nameBytes.Length);
            UInt16 numAncestors = Convert.ToUInt16(ancestors != null ? ancestors.Count : 0);
            dataWriter.Write(GetIdNumber());
            dataWriter.Write(level);
            dataWriter.Write(nameLength);
            dataWriter.Write(numAncestors);
            dataWriter.Write(nameBytes);
            for (UInt16 idxAncestor = 0; idxAncestor < numAncestors; idxAncestor++)
            {
                dataWriter.Write(ancestors[idxAncestor].GetIdNumber());
            }
        }

        public static void Extract(string inputPath, string outputPath)
        {
            string conceptsPath = Path.Combine(inputPath, "concepts");
            DirectoryInfo diConceptsPath = new DirectoryInfo(conceptsPath);
            foreach (var conceptDir in diConceptsPath.GetDirectories())
            {
                foreach (FileInfo conceptFile in conceptDir.GetFiles())
                {
                    if (conceptFile.Extension == ".gz")
                    {
                        Console.WriteLine(conceptFile.FullName);
                        using (FileStream conceptDataFile = new FileStream(Path.Combine(outputPath, $"concept-data.wjf"), FileMode.Append))
                        {
                            using (BinaryWriter conceptDataWriter = new BinaryWriter(conceptDataFile))
                            {
                                using (FileStream conceptIndexFile = new FileStream(Path.Combine(outputPath, $"concept-index.wjf"), FileMode.Append))
                                {
                                    using (BinaryWriter conceptIndexWriter = new BinaryWriter(conceptIndexFile))
                                    {
                                        using (FileStream gzFile = File.OpenRead(conceptFile.FullName))
                                        {
                                            using (GZipStream gzStream = new GZipStream(gzFile, CompressionMode.Decompress))
                                            {
                                                using (StreamReader reader = new StreamReader(gzStream))
                                                {
                                                    string? line = reader.ReadLine();
                                                    while (line != null)
                                                    {
                                                        try
                                                        {
                                                            OpenAlexDataExtractor.Concept concept = JsonConvert.DeserializeObject<OpenAlexDataExtractor.Concept>(line);
                                                            concept.Serialize(conceptDataFile.Position, conceptDataWriter, conceptIndexWriter);
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            Console.WriteLine(e.Message);
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
                }
            }
        }
    }

    public class XConcept
    {
        [BsonElement("concept_id")]
        public string id { get; set; }
        [BsonElement("name")]
        public string display_name { get; set; }
        [BsonElement("level")]
        public UInt16 level { get; set; }
        [BsonElement("score")]
        public double score { get; set; }

        public UInt64 GetIdNumber()
        {
            return GetIdNumber(id);
        }

        UInt64 GetIdNumber(string idStr)
        {
            if (idStr == null)
                return 0;
            string[] idParts = idStr.Split('/');
            UInt64 idNumber = Convert.ToUInt64(idParts[idParts.Length - 1].Substring(1));
            return idNumber;
        }

        Byte[] GetNameBytes()
        {
            return Encoding.UTF8.GetBytes(display_name);
        }

        
    }
}
