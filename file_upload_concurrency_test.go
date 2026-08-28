package proton_api_bridge

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rclone/go-proton-api"
	"golang.org/x/sync/semaphore"
)

func testPendingBlocks(indexes ...int) []pendingUploadBlock {
	blocks := make([]pendingUploadBlock, len(indexes))
	for i, index := range indexes {
		blocks[i] = pendingUploadBlock{
			blockUploadInfo: proton.BlockUploadInfo{Index: index},
			encData:         []byte{byte(index)},
		}
	}
	return blocks
}

func testUploadLinks(blocks []proton.BlockUploadInfo) []proton.BlockUploadLink {
	links := make([]proton.BlockUploadLink, len(blocks))
	for i, block := range blocks {
		links[i] = proton.BlockUploadLink{Token: string(rune(block.Index))}
	}
	return links
}

func noRetryWait(_ context.Context, _ time.Duration) error { return nil }

func TestUploadBlockBatchRetriesOnlyTransientFailures(t *testing.T) {
	var requestIndexes [][]int
	requestLinks := func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
		indexes := make([]int, len(blocks))
		for i, block := range blocks {
			indexes[i] = block.Index
		}
		requestIndexes = append(requestIndexes, indexes)
		return testUploadLinks(blocks), nil
	}

	var mu sync.Mutex
	uploads := map[int]int{}
	uploadBlock := func(_ context.Context, _ proton.BlockUploadLink, block []byte) error {
		index := int(block[0])
		mu.Lock()
		uploads[index]++
		attempt := uploads[index]
		mu.Unlock()
		if attempt == 1 && index != 2 {
			return &proton.APIError{Status: 502, Message: "temporary storage failure"}
		}
		return nil
	}

	err := uploadBlockBatchWithRetry(
		context.Background(),
		testPendingBlocks(1, 2, 3),
		blockUploadMaxAttempts,
		requestLinks,
		uploadBlock,
		noRetryWait,
		nil,
	)
	if err != nil {
		t.Fatalf("retrying transient block uploads failed: %v", err)
	}
	if want := [][]int{{1, 2, 3}, {1, 3}}; !reflect.DeepEqual(requestIndexes, want) {
		t.Fatalf("requested block indexes %v, want %v", requestIndexes, want)
	}
	if want := map[int]int{1: 2, 2: 1, 3: 2}; !reflect.DeepEqual(uploads, want) {
		t.Fatalf("block upload counts %v, want %v", uploads, want)
	}
}

func TestUploadBlockBatchReturnsNonRetryableError(t *testing.T) {
	terminalErr := &proton.APIError{Status: 422, Message: "draft conflict"}
	requests := 0
	err := uploadBlockBatchWithRetry(
		context.Background(),
		testPendingBlocks(1),
		blockUploadMaxAttempts,
		func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			requests++
			return testUploadLinks(blocks), nil
		},
		func(_ context.Context, _ proton.BlockUploadLink, _ []byte) error { return terminalErr },
		noRetryWait,
		nil,
	)
	if !errors.Is(err, terminalErr) {
		t.Fatalf("returned error %v, want %v", err, terminalErr)
	}
	if requests != 1 {
		t.Fatalf("requested upload links %d times after a terminal error, want 1", requests)
	}
}

func TestUploadBlockBatchRetriesTransientLinkRequest(t *testing.T) {
	transientErr := &proton.APIError{Status: 502, Message: "temporary API failure"}
	requests := 0
	uploads := 0
	err := uploadBlockBatchWithRetry(
		context.Background(),
		testPendingBlocks(1),
		blockUploadMaxAttempts,
		func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			requests++
			if requests == 1 {
				return nil, transientErr
			}
			return testUploadLinks(blocks), nil
		},
		func(_ context.Context, _ proton.BlockUploadLink, _ []byte) error {
			uploads++
			return nil
		},
		noRetryWait,
		nil,
	)
	if err != nil {
		t.Fatalf("retrying a transient link request failed: %v", err)
	}
	if requests != 2 || uploads != 1 {
		t.Fatalf("observed %d link requests and %d uploads, want 2 and 1", requests, uploads)
	}
}

func TestUploadBlockBatchRejectsMismatchedLinkCount(t *testing.T) {
	err := uploadBlockBatchWithRetry(
		context.Background(),
		testPendingBlocks(1, 2),
		blockUploadMaxAttempts,
		func(_ context.Context, _ []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			return []proton.BlockUploadLink{{}}, nil
		},
		func(_ context.Context, _ proton.BlockUploadLink, _ []byte) error {
			t.Fatal("upload must not start with a mismatched link response")
			return nil
		},
		noRetryWait,
		nil,
	)
	if err == nil {
		t.Fatal("expected a mismatched link count to fail")
	}
}

func TestUploadBlockBatchReturnsLastErrorAfterLimit(t *testing.T) {
	transientErr := &proton.APIError{Status: 502, Message: "temporary storage failure"}
	requests := 0
	err := uploadBlockBatchWithRetry(
		context.Background(),
		testPendingBlocks(1),
		3,
		func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			requests++
			return testUploadLinks(blocks), nil
		},
		func(_ context.Context, _ proton.BlockUploadLink, _ []byte) error { return transientErr },
		noRetryWait,
		nil,
	)
	if !errors.Is(err, transientErr) {
		t.Fatalf("returned error %v, want %v", err, transientErr)
	}
	if requests != 3 {
		t.Fatalf("requested upload links %d times, want 3", requests)
	}
}

func TestUploadBlockBatchHonorsCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := uploadBlockBatchWithRetry(
		ctx,
		testPendingBlocks(1),
		blockUploadMaxAttempts,
		func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			return testUploadLinks(blocks), nil
		},
		func(_ context.Context, _ proton.BlockUploadLink, _ []byte) error {
			return &proton.APIError{Status: 502, Message: "temporary storage failure"}
		},
		waitForBlockUploadRetry,
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("returned error %v, want context cancellation", err)
	}
}

func TestUploadBlockBatchReleasesAllWorkersAfterFailure(t *testing.T) {
	const slotCount = int64(20)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	slots := semaphore.NewWeighted(slotCount)

	for batch := 0; batch < 4; batch++ {
		err := uploadBlockBatchWithRetry(
			ctx,
			testPendingBlocks(1, 2, 3, 4, 5, 6, 7, 8),
			blockUploadMaxAttempts,
			func(_ context.Context, blocks []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
				return testUploadLinks(blocks), nil
			},
			func(_ context.Context, _ proton.BlockUploadLink, block []byte) error {
				if err := slots.Acquire(ctx, 1); err != nil {
					return err
				}
				defer slots.Release(1)
				if block[0] == 1 {
					return errors.New("synthetic upload failure")
				}
				return nil
			},
			noRetryWait,
			nil,
		)
		if err == nil {
			t.Fatal("expected the first upload failure to be returned")
		}
	}

	if err := slots.Acquire(ctx, slotCount); err != nil {
		t.Fatalf("upload workers leaked semaphore slots: %v", err)
	}
	slots.Release(slotCount)
}
