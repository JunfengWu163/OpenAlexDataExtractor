using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenAlexDataExtractor
{
    public class ConceptEntity
    {
        public UInt64 id { get; private set; }
        public UInt16 level { get; private set; }
        public string name { get; private set; }
        public List<UInt64> ancestorIds { get; private set; } = new List<UInt64>();

        public ConceptEntity(UInt64 idNumber, string path)
        {
            string indexFileName = Path.Combine(path, "concept-sorted_index.wjf");
            Int64 conceptPos = FindConcept(indexFileName, idNumber);
            if (conceptPos >= 0)
            {
                string dataFileName = Path.Combine(path, "concept-data.wjf");
                Deserialize(dataFileName, idNumber, conceptPos);
            }
            else
            {
                id = 0;
            }
        }

        public ConceptEntity(string fileName, UInt64 idNumber, Int64 conceptPos)
        {
            Deserialize(fileName, idNumber, conceptPos);
        }

        Int64 FindConcept(string indexFileName, UInt64 idNumber)
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

        Byte[] GetNameBytes()
        {
            return Encoding.UTF8.GetBytes(name);
        }

        void Deserialize(string fileName, UInt64 idNumber, Int64 conceptPos)
        {
            using (FileStream dataFile = File.OpenRead(fileName))
            {
                dataFile.Seek(conceptPos, SeekOrigin.Begin);
                using (BinaryReader dataReader = new BinaryReader(dataFile))
                {
                    id = dataReader.ReadUInt64();
                    Debug.Assert(idNumber == id);

                    level = dataReader.ReadUInt16();

                    UInt16 nameLength = dataReader.ReadUInt16();
                    UInt16 numAncestors = dataReader.ReadUInt16();

                    Byte[] nameBytes = new Byte[nameLength];
                    dataReader.Read(nameBytes);
                    name = Encoding.UTF8.GetString(nameBytes);

                    for (UInt16 idxAncestor = 0; idxAncestor < numAncestors; idxAncestor++)
                    {
                        UInt64 ancestorId = dataReader.ReadUInt64();
                        ancestorIds.Add(ancestorId);
                    }
                }
            }
        }

        public void Serialize(Int64 conceptPos, BinaryWriter dataWriter, BinaryWriter indexWriter)
        {
            indexWriter.Write(id);
            indexWriter.Write(conceptPos);

            Byte[] nameBytes = GetNameBytes();
            UInt16 nameLength = Convert.ToUInt16(nameBytes.Length);
            UInt16 numAncestors = Convert.ToUInt16(ancestorIds.Count);
            dataWriter.Write(id);
            dataWriter.Write(level);
            dataWriter.Write(nameLength);
            dataWriter.Write(numAncestors);
            dataWriter.Write(nameBytes);
            for (UInt16 idxAncestor = 0; idxAncestor < numAncestors; idxAncestor++)
            {
                dataWriter.Write(ancestorIds[idxAncestor]);
            }
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
                        Int64 conceptPos = br.ReadInt64();
                        indices.Add((idNumber, conceptPos));
                    }
                }
            }
            return indices;
        }

        public static void SortIndices(string outputPath)
        {
            string indexFileName = Path.Combine(outputPath, $"concept-index.wjf");
            string sortedIndexFileName = Path.Combine(outputPath, $"concept-sorted_index.wjf");
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
