using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace OpenAlexDataExtractor
{
    public class VenueEntity
    {
        public UInt64 id { get; private set; }
        public string name { get; private set; }

        public VenueEntity(UInt64 idNumber, string path)
        {
            string indexFileName = Path.Combine(path, "venue-sorted_index.wjf");
            Int64 venuePos = FindVenue(indexFileName, idNumber);
            if (venuePos >= 0)
            {
                string dataFileName = Path.Combine(path, "venue-data.wjf");
                Deserialize(dataFileName, idNumber, venuePos);
            }
            else
            {
                id = 0;
            }
        }

        public VenueEntity(string fileName, UInt64 idNumber, Int64 venuePos)
        {
            Deserialize(fileName, idNumber, venuePos);
        }

        Byte[] GetNameBytes()
        {
            return Encoding.UTF8.GetBytes(name);
        }

        Int64 FindVenue(string indexFileName, UInt64 idNumber)
        {
            if (File.Exists(indexFileName))
            {
                using (FileStream indexFile = File.OpenRead(indexFileName))
                {
                    Int64 numIndices = indexFile.Length >> 4;
                    if (numIndices == 0)
                    {
                        return -1;
                    }

                    Int64 iStart = 0, iEnd = numIndices - 1;
                    using (BinaryReader reader = new BinaryReader(indexFile))
                    {
                        UInt64 indexStart = reader.ReadUInt64();
                        if (idNumber == indexStart)
                        {
                            return reader.ReadInt64();
                        }
                        else if (idNumber < indexStart)
                        {
                            return -1;
                        }

                        indexFile.Seek(iEnd << 4, SeekOrigin.Begin);
                        UInt64 indexEnd = reader.ReadUInt64();
                        if (idNumber == indexEnd)
                        {
                            return reader.ReadInt64();
                        }
                        else if (idNumber > indexEnd)
                        {
                            return -1;
                        }

                        while (iEnd - iStart >= 2)
                        {
                            Int64 iMiddle = (iStart + iEnd) >> 1;
                            indexFile.Seek(iMiddle << 4, SeekOrigin.Begin);
                            UInt64 indexMiddle = reader.ReadUInt64();
                            if (idNumber == indexMiddle)
                            {
                                return reader.ReadInt64();
                            }
                            else if (idNumber < indexMiddle)
                            {
                                iEnd = iMiddle;
                                indexEnd = indexMiddle;
                            }
                            else
                            {
                                iStart = iMiddle;
                                indexStart = indexMiddle;
                            }
                        }

                        // now iEnd - iStart < 2, but idNumber != indexStart && idNumber != indexEnd, thus idNumber is not found
                        return -1;
                    }
                }
            }
            return -1;
        }

        void Deserialize(string fileName, UInt64 idNumber, Int64 venuePos)
        {
            using (FileStream dataFile = File.OpenRead(fileName))
            {
                dataFile.Seek(venuePos, SeekOrigin.Begin);
                using (BinaryReader dataReader = new BinaryReader(dataFile))
                {
                    id = dataReader.ReadUInt64();
                    Debug.Assert(idNumber == id);

                    UInt16 nameLength = dataReader.ReadUInt16();

                    Byte[] nameBytes = new Byte[nameLength];
                    dataReader.Read(nameBytes);
                    name = Encoding.UTF8.GetString(nameBytes);
                }
            }
        }

        public void Serialize(Int64 venuePos, BinaryWriter dataWriter, BinaryWriter indexWriter)
        {
            indexWriter.Write(id);
            indexWriter.Write(venuePos);

            Byte[] nameBytes = GetNameBytes();
            UInt16 nameLength = Convert.ToUInt16(nameBytes.Length);
            dataWriter.Write(id);
            dataWriter.Write(nameLength);
            dataWriter.Write(nameBytes);
        }

        public static List<(UInt64, Int64)> LoadIndices(string fileName)
        {
            List<(UInt64, Int64)> indices = new List<(UInt64, Int64)>();
            using (FileStream fs = File.OpenRead(fileName))
            {
                Int64 fileSize = fs.Length;
                Int64 numIndices = fileSize / 16;
                using (BinaryReader br = new BinaryReader(fs))
                {
                    for (Int64 i = 0; i < numIndices; i++)
                    {
                        UInt64 idNumber = br.ReadUInt64();
                        Int64 venuePos = br.ReadInt64();
                        indices.Add((idNumber, venuePos));
                    }
                }
            }
            return indices;
        }

        public static void SortIndices(string outputPath)
        {
            string indexFileName = Path.Combine(outputPath, $"venue-index.wjf");
            string sortedIndexFileName = Path.Combine(outputPath, $"venue-sorted_index.wjf");
            List<(UInt64, Int64)> indices = LoadIndices(indexFileName);
            indices.Sort((x, y) => Math.Sign(Convert.ToInt64(x.Item1) - Convert.ToInt64(y.Item1)));
            using (FileStream fileOut = File.Create(sortedIndexFileName))
            {
                using (BinaryWriter indexWriter = new BinaryWriter(fileOut))
                {
                    for (int i = 0; i < indices.Count; i++)
                    {
                        indexWriter.Write(indices[i].Item1);
                        indexWriter.Write(indices[i].Item2);
                    }
                }
            }
        }
    }
}
