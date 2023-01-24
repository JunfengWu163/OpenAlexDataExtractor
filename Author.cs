using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Threading;

namespace OpenAlexDataExtractor
{
    public class Author
    {
        [BsonElement("id")]
        public string id { get; set; }
        [BsonElement("display_name")]
        public string display_name { get; set; }
        [BsonElement("orcid")]
        public string orcid { get; set; }

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

        public void UpdateAuthors(ConcurrentDictionary<UInt64, string> authors)
        {
            if (display_name == null)
                return;

            UInt64 idNumber = GetIdNumber();
            if (!authors.ContainsKey(idNumber))
            {
                authors[idNumber] = display_name;
            }
        }

        public static void Serialize(ConcurrentDictionary<UInt64, string> authors, string outputPath, int numThreads)
        {
            using (FileStream authorDataFile = new FileStream(Path.Combine(outputPath, $"author-data.wjf"), FileMode.Create))
            {
                using (BinaryWriter authorDataWriter = new BinaryWriter(authorDataFile))
                {
                    using (FileStream authorIndexFile = new FileStream(Path.Combine(outputPath, $"author-index.wjf"), FileMode.Create))
                    {
                        using (BinaryWriter authorIndexWriter = new BinaryWriter(authorIndexFile))
                        {
                            foreach (var kv in authors)
                            {
                                if (kv.Value == null) continue;
                                UInt64 idNumber = kv.Key;
                                Byte[] nameBytes = Encoding.UTF8.GetBytes(kv.Value);
                                Int32 nameLength = nameBytes.Length;
                                Int64 position = authorDataFile.Position;

                                authorIndexWriter.Write(idNumber);
                                authorIndexWriter.Write(position);
                                authorDataWriter.Write(idNumber);
                                authorDataWriter.Write(nameLength);
                                authorDataWriter.Write(nameBytes);
                            }
                            
                        }
                    }
                }
            }
        }
    }

    public class Authorship
    {
        [BsonElement("author_position")]
        public string author_position { get; set; }
        [BsonElement("author")]
        public Author author { get; set; }
        public UInt16 GetPositionNumber()
        {
            switch (author_position)
            {
                case "first":
                    return 1;
                case "middle":
                    return 2;
                case "last":
                    return 3;
                default:
                    return 0;
            }
        }
    }
}
