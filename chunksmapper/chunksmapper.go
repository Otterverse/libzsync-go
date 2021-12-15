package chunksmapper

import (
	"github.com/AppImageCrafters/libzsync-go/chunks"
	"sort"
	//"fmt"
)

type ChunksMapper struct {
	fileSize  int64
	chunksMap map[int64]chunks.ChunkInfo
}

func NewFileChunksMapper(fileSize int64) *ChunksMapper {
	return &ChunksMapper{fileSize: fileSize, chunksMap: make(map[int64]chunks.ChunkInfo)}
}

func (mapper *ChunksMapper) FillChunksMap(chunkChannel <-chan chunks.ChunkInfo) {
	for {
		chunk, ok := <-chunkChannel
		if ok == false {
			break
		}

		mapper.Add(chunk)
	}
}

func (mapper *ChunksMapper) GetMappedChunks() []chunks.ChunkInfo {
	var chunkList []chunks.ChunkInfo
	for _, chk := range mapper.chunksMap {
		chunkList = append(chunkList, chk)
	}

	sort.SliceStable(chunkList, func(i, j int) bool {
		return chunkList[i].TargetOffset < chunkList[j].TargetOffset
	})

	return chunkList
}

func (mapper *ChunksMapper) GetMissingChunks() []chunks.ChunkInfo {
	chunkList := mapper.GetMappedChunks()
	var missingChunkList []chunks.ChunkInfo

	pastChunkEnd := int64(0)
	for _, c := range chunkList {
		if pastChunkEnd != c.TargetOffset {
			missingChunkList = append(missingChunkList, chunks.ChunkInfo{
				Size:         c.TargetOffset - pastChunkEnd,
				SourceOffset: pastChunkEnd,
				TargetOffset: pastChunkEnd,
			})
		}

		pastChunkEnd = c.TargetOffset + c.Size
	}

	if pastChunkEnd != mapper.fileSize {
		missingChunkList = append(missingChunkList, chunks.ChunkInfo{
			Size:         mapper.fileSize - pastChunkEnd,
			SourceOffset: pastChunkEnd,
			TargetOffset: pastChunkEnd,
		})
	}

	return missingChunkList
}

func (mapper *ChunksMapper) Add(chunk chunks.ChunkInfo) {
	mapper.chunksMap[chunk.TargetOffset] = chunk
}

func (mapper *ChunksMapper) OptimizeChunks(chunkList []chunks.ChunkInfo, minGap int64) []chunks.ChunkInfo {
	//fmt.Println("numChunks in: ", len(chunkList))
	var chunkListOut []chunks.ChunkInfo
	skipList := make(map[int]bool)
	for i := 0; i < len(chunkList); i++ {
		if skipList[i] {
			continue
		}
		chunk := chunkList[i]
		end := chunk.SourceOffset + chunk.Size

		for n := i; n < len(chunkList); n++ {
			if skipList[n] {
				continue
			}
			nextChunk := chunkList[n]
			nextEnd := nextChunk.SourceOffset + nextChunk.Size

			if end + minGap > nextChunk.SourceOffset {
				end = nextEnd
				chunk.Size = end - chunk.SourceOffset
				skipList[n] = true
			}
		}
		chunkListOut = append(chunkListOut, chunk)
	}
	//fmt.Println("numChunks out: ", len(chunkListOut))

	// for _, c := range chunkList {
	// 	fmt.Println("ChunksIn: ", c.SourceOffset, c.SourceOffset+c.Size, c.Size)
	// }

	// for _, c := range chunkListOut {
	// 	fmt.Println("ChunksOut: ", c.SourceOffset, c.SourceOffset+c.Size, c.Size)
	// }
	return chunkListOut
}