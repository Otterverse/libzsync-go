package sources

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type HttpFileSource struct {
	URL    string
	Offset int64
	Size   int64

	cacheBegin  int64
	cacheEnd    int64
	readerCache bytes.Buffer
	client      *http.Client
}

func (h *HttpFileSource) Read(b []byte) (n int, err error) {
	if h.readerCache.Len() != 0 &&
		(h.Offset < h.cacheBegin || h.Offset+int64(len(b)) > h.cacheEnd) {
		h.readerCache.Reset()
	}

	if h.readerCache.Len() == 0 {
		err = h.Request(int64(len(b)))
		if err != nil {
			return 0, err
		}
	}

	n, err = h.readerCache.Read(b)
	_, _ = h.Seek(int64(n), 1)
	return n, err
}

func (h *HttpFileSource) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		h.Offset = offset
	case 1:
		h.Offset += offset
	case 2:
		h.Offset = h.Size + offset
	default:
		return -1, fmt.Errorf("Unknown whence value: %d", whence)
	}

	return offset, nil
}

func (h *HttpFileSource) Request(size int64) (err error) {
	h.cacheBegin = h.Offset
	h.cacheEnd = h.Offset + size

	body, err := h.doRangeRequest(h.cacheBegin, h.cacheEnd)
	if err != nil {
		return err
	}
	defer body.Close()

	n, err := h.readerCache.ReadFrom(body)
	if err != nil {
		return err
	}

	if n != size {
		return fmt.Errorf("Range size mismatch. Got: %d, expected %d", n, size)
	}

	return nil
}

func (h *HttpFileSource) doRangeRequest(range_start int64, range_end int64) (io.ReadCloser, error) {
	// fmt.Println("Requesting chunk: ", range_start, range_end, range_end-range_start)
	rangedRequest, err := http.NewRequest("GET", h.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating range request for \"%v\": %v", h.URL, err)
	}

	rangeSpecifier := fmt.Sprintf("bytes=%v-%v", range_start, range_end-1)
	rangedRequest.ProtoAtLeast(1, 1)
	rangedRequest.Header.Add("Range", rangeSpecifier)
	rangedRequest.Header.Add("Accept-Encoding", "identity")

	if h.client == nil {
		h.client = &http.Client{}
	}

	rangedResponse, err := h.client.Do(rangedRequest)
	if err != nil {
		return nil, fmt.Errorf("Error executing request for \"%v\": %v", h.URL, err)
	}

	if rangedResponse.StatusCode == 404 {
		return nil, fmt.Errorf("URL not found")
	}

	if rangedResponse.StatusCode != 206 {
		return nil, fmt.Errorf("ranged request not supported")
	}

	if strings.Contains(rangedResponse.Header.Get("Content-Encoding"), "gzip") {
		return nil, fmt.Errorf("response from server was GZiped")
	}

	return rangedResponse.Body, nil
}
