package zsync

/**
zsync is a file transfer program. It allows you to download a file from a remote server, where you have a copy of an
older version of the file on your computer already. zsync downloads only the new parts of the file. It uses the same
algorithm as rsync. However, where rsync is designed for synchronising data from one computer to another within an
organisation, zsync is designed for file distribution, with one file on a server to be distributed to thousands of
downloaders. zsync requires no special server software "just a web server to host the files" and imposes no extra
load on the server, making it ideal for large scale file distribution.

See http://zsync.moria.org.uk/
*/

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/Otterverse/libzsync-go/chunks"
	"github.com/Otterverse/libzsync-go/chunksmapper"
	"github.com/Otterverse/libzsync-go/control"
	"github.com/Otterverse/libzsync-go/hasedbuffer"
	"github.com/Otterverse/libzsync-go/index"
	"github.com/Otterverse/libzsync-go/sources"
)

type ZSync struct {
	BlockSize      int64
	ChecksumsIndex *index.ChecksumIndex

	RemoteFileUrl  string
	RemoteFileSize int64
	RemoteFileSHA1 string
}

func NewZSync(zsyncFileUrl string) (*ZSync, error) {
	resp, err := http.Get(zsyncFileUrl)
	if err != nil {
		return nil, err
	}

	c, err := control.ReadControl(resp.Body)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	// Relative URLs
	if !(strings.HasPrefix(c.URL, "http") || strings.HasPrefix(c.URL, "ftp")) {
		baseURL := strings.LastIndex(zsyncFileUrl, "/")
		c.URL = zsyncFileUrl[:baseURL] + "/" + c.URL
	}

	return &ZSync{
		BlockSize:      int64(c.BlockSize),
		ChecksumsIndex: c.ChecksumIndex,
		RemoteFileUrl:  c.URL,
		RemoteFileSize: c.FileLength,
		RemoteFileSHA1: c.SHA1,
	}, nil
}

func NewZSyncFromControl(c *control.Control) *ZSync {
	return &ZSync{
		BlockSize:      int64(c.BlockSize),
		ChecksumsIndex: c.ChecksumIndex,
		RemoteFileUrl:  c.URL,
		RemoteFileSize: c.FileLength,
	}
}

func (zsync *ZSync) Sync(filePath string, output io.WriteSeeker) error {
	reusableChunks, err := zsync.SearchReusableChunks(filePath)
	if err != nil {
		return err
	}

	input, err := os.Open(filePath)
	if err != nil {
		return err
	}

	chunkMapper := chunksmapper.NewFileChunksMapper(zsync.RemoteFileSize)
	for chunk := range reusableChunks {
		err = zsync.WriteChunk(input, output, chunk)
		if err != nil {
			return err
		}

		chunkMapper.Add(chunk)
	}

	missingChunksSource := sources.HttpFileSource{URL: zsync.RemoteFileUrl, Size: zsync.RemoteFileSize}
	missingChunks := chunkMapper.GetMissingChunks()
	missingChunks = chunkMapper.OptimizeChunks(missingChunks, zsync.BlockSize*64)

	for _, chunk := range missingChunks {
		// fetch whole chunk to reduce the number of request
		_, err = missingChunksSource.Seek(chunk.SourceOffset, io.SeekStart)
		if err != nil {
			return err
		}

		err = missingChunksSource.Request(chunk.Size)
		if err != nil {
			return err
		}

		err = zsync.WriteChunk(&missingChunksSource, output, chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (zsync *ZSync) SearchReusableChunks(path string) (<-chan chunks.ChunkInfo, error) {
	inputSize, err := zsync.getFileSize(path)
	if err != nil {
		return nil, err
	}

	nChunks := inputSize / zsync.BlockSize
	if nChunks*zsync.BlockSize < inputSize {
		nChunks++
	}

	nWorkers := int64(runtime.NumCPU())
	if nWorkers > nChunks {
		nWorkers = nChunks
	}

	nChunksPerWorker := nChunks / nWorkers

	chunkChannel := make(chan chunks.ChunkInfo)
	var waitGroup sync.WaitGroup

	waitGroup.Add(int(nWorkers))

	for i := int64(0); i < nWorkers; i++ {
		begin := (nChunksPerWorker * zsync.BlockSize) * i

		end := begin + nChunksPerWorker*zsync.BlockSize
		if end > inputSize {
			end = inputSize
		}

		go zsync.searchReusableChunksAsync(path, begin, end, chunkChannel, &waitGroup)
	}

	go func() {
		waitGroup.Wait()
		close(chunkChannel)
	}()

	return chunkChannel, nil
}

func (zsync *ZSync) getFileSize(filePath string) (int64, error) {
	inputStat, err := os.Stat(filePath)
	if err != nil {
		return -1, err
	}

	return inputStat.Size(), nil
}

func (zsync *ZSync) searchReusableChunksAsync(path string, begin int64, end int64, chunksChan chan<- chunks.ChunkInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	input, err := os.Open(path)
	if err != nil {
		return
	}

	_, err = input.Seek(begin, io.SeekStart)
	if err != nil {
		return
	}

	nextStep := zsync.BlockSize
	buf := hasedbuffer.NewHashedBuffer(int(zsync.BlockSize))

	for off := begin; off < end; off += nextStep {
		err := zsync.consumeBytes(buf, input, nextStep)
		if err != nil {
			break
		}

		weakSum := buf.RollingSum()
		weakMatches := zsync.ChecksumsIndex.FindWeakChecksum2(weakSum)

		if weakMatches != nil {
			strongSum := buf.CheckSum()
			strongMatches := zsync.ChecksumsIndex.FindStrongChecksum2(strongSum, weakMatches)
			if strongMatches != nil {
				zsync.createChunks(strongMatches, off, chunksChan)

				// consume entire block
				nextStep = zsync.BlockSize
				continue
			}
		}

		// just consume 1 byte
		nextStep = 1
	}

	_ = input.Close()
}

func (zsync *ZSync) consumeBytes(buf *hasedbuffer.HashedRingBuffer, input *os.File, nBytes int64) error {
	if nBytes == zsync.BlockSize {
		_, err := buf.ReadFull(input)
		return err
	} else {
		for i := int64(0); i < nBytes; i++ {
			_, err := buf.ReadByte(input)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (zsync *ZSync) createChunks(strongMatches []chunks.ChunkChecksum, offset int64, chunksChan chan<- chunks.ChunkInfo) {
	for _, match := range strongMatches {
		newChunk := chunks.ChunkInfo{
			Size:         zsync.BlockSize,
			Source:       nil,
			SourceOffset: offset,
			TargetOffset: int64(match.ChunkOffset) * zsync.BlockSize,
		}

		// chop zero filled chunks at the end
		if newChunk.TargetOffset+newChunk.Size > zsync.RemoteFileSize {
			newChunk.Size = zsync.RemoteFileSize - newChunk.TargetOffset
		}

		chunksChan <- newChunk
	}
}

func (zsync *ZSync) WriteChunk(source io.ReadSeeker, target io.WriteSeeker, chunk chunks.ChunkInfo) error {
	_, err := source.Seek(chunk.SourceOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("unable to seek source offset: %d", chunk.SourceOffset)
	}

	_, err = target.Seek(chunk.TargetOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("unable to seek target offset: %d", chunk.TargetOffset)
	}

	n, err := io.CopyN(target, source, chunk.Size)
	// Special case when the checksum buffer (zero-filled) matches at the end of a file
	if err == io.EOF && n < chunk.Size {
		zeros := make([]byte, chunk.Size-n)
		_, err = target.Write(zeros)
	}
	if err != nil {
		return fmt.Errorf("unable to copy bytes: %d %s", n, err.Error())
	}

	return nil
}
