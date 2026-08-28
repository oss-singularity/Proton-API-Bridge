package proton_api_bridge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"time"

	"github.com/ProtonMail/gopenpgp/v3/crypto"
	"github.com/rclone/Proton-API-Bridge/utility"
	"github.com/rclone/go-proton-api"
)

const (
	blockUploadMaxAttempts    = 5
	blockUploadRetryBaseDelay = time.Second
	blockUploadRetryMaxDelay  = 15 * time.Second
)

type pendingUploadBlock struct {
	blockUploadInfo proton.BlockUploadInfo
	encData         []byte
}

type blockUploadResult struct {
	index int
	err   error
}

type blockUploadRetryLogger interface {
	Warnf(format string, v ...interface{})
}

func retryableBlockUploadError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var apiErr *proton.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status >= 500 && apiErr.Status <= 599
	}

	var protonNetErr *proton.NetError
	return errors.As(err, &protonNetErr)
}

func blockUploadRetryDelay(failedAttempt int) time.Duration {
	delay := blockUploadRetryBaseDelay
	for i := 1; i < failedAttempt && delay < blockUploadRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > blockUploadRetryMaxDelay {
		return blockUploadRetryMaxDelay
	}
	return delay
}

func waitForBlockUploadRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func uploadBlockBatchWithRetry(
	ctx context.Context,
	blocks []pendingUploadBlock,
	maxAttempts int,
	requestLinks func(context.Context, []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error),
	uploadBlock func(context.Context, proton.BlockUploadLink, []byte) error,
	wait func(context.Context, time.Duration) error,
	logger blockUploadRetryLogger,
) error {
	remaining := append([]pendingUploadBlock(nil), blocks...)
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		blockList := make([]proton.BlockUploadInfo, len(remaining))
		for i := range remaining {
			blockList[i] = remaining[i].blockUploadInfo
		}

		links, err := requestLinks(ctx, blockList)
		if err != nil {
			lastErr = err
			if !retryableBlockUploadError(err) || attempt == maxAttempts {
				return err
			}
		} else {
			if len(links) != len(remaining) {
				return fmt.Errorf(
					"requested %d Proton block upload links, received %d",
					len(remaining),
					len(links),
				)
			}

			results := make(chan blockUploadResult, len(remaining))
			for i := range remaining {
				go func(index int) {
					results <- blockUploadResult{
						index: index,
						err:   uploadBlock(ctx, links[index], remaining[index].encData),
					}
				}(i)
			}

			errorsByIndex := make([]error, len(remaining))
			for range remaining {
				result := <-results
				errorsByIndex[result.index] = result.err
			}

			failed := make([]pendingUploadBlock, 0, len(remaining))
			var terminalErr error
			lastErr = nil
			for i, uploadErr := range errorsByIndex {
				if uploadErr == nil {
					continue
				}
				if !retryableBlockUploadError(uploadErr) && terminalErr == nil {
					terminalErr = uploadErr
				}
				if lastErr == nil {
					lastErr = uploadErr
				}
				failed = append(failed, remaining[i])
			}
			if terminalErr != nil {
				return terminalErr
			}
			if len(failed) == 0 {
				return nil
			}
			if attempt == maxAttempts {
				return lastErr
			}
			remaining = failed
		}

		delay := blockUploadRetryDelay(attempt)
		if logger != nil {
			logger.Warnf(
				"Retrying %d transient Proton block upload(s) after %s (attempt %d/%d)",
				len(remaining),
				delay,
				attempt+1,
				maxAttempts,
			)
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}

	return lastErr
}

func (protonDrive *ProtonDrive) handleRevisionConflict(ctx context.Context, link *proton.Link, createFileResp *proton.CreateFileRes) (string, bool, error) {
	if link != nil {
		linkID := link.LinkID

		// A draft link (no active revision) is a leftover from a failed or
		// incomplete upload. Listing the revisions of such a link returns a
		// "File or folder not found" (2501) error, so don't: handle it
		// directly by deleting the link and resubmitting the file creation.
		// There is a restriction of one draft revision per file.
		if link.State == proton.LinkStateDraft {
			// TODO: maintain clientUID to mark that this is our own draft (which can indicate failed upload attempt!)
			if !protonDrive.Config.ReplaceExistingDraft {
				// based on the web behavior, it will ask if the user wants to replace the failed upload attempt
				// current behavior, we report an error to not upload the file (conservative)
				return "", false, ErrDraftExists
			}

			// delete the link (skipping trash, otherwise it won't work) and
			// signal the caller to resubmit the file creation request
			err := protonDrive.c.DeleteChildren(ctx, protonDrive.MainShare.ShareID, link.ParentLinkID, linkID)
			if err != nil {
				return "", false, err
			}

			return "", true, nil
		}

		// The link has an active revision. A concurrent/failed upload may also
		// have left a draft revision on it; depending on the user config, we
		// can abort the upload or delete that draft revision before creating a
		// new one.
		draftRevision, err := protonDrive.GetRevisions(ctx, link, proton.RevisionStateDraft)
		if err != nil {
			return "", false, err
		}

		if len(draftRevision) > 0 {
			if !protonDrive.Config.ReplaceExistingDraft {
				return "", false, ErrDraftExists
			}

			// Question: how do we observe for file upload cancellation -> clientUID?
			// Random thoughts: if there are concurrent modification to the draft, the server should be able to catch this when commiting the revision
			// since the manifestSignature (hash) will fail to match
			err = protonDrive.c.DeleteRevision(ctx, protonDrive.MainShare.ShareID, linkID, draftRevision[0].ID)
			if err != nil {
				return "", false, err
			}
		}

		// create a new revision
		newRevision, err := protonDrive.c.CreateRevision(ctx, protonDrive.MainShare.ShareID, linkID)
		if err != nil {
			return "", false, err
		}

		return newRevision.ID, false, nil
	} else if createFileResp != nil {
		return createFileResp.RevisionID, false, nil
	} else {
		// should not happen anymore, since the file search will include the draft now
		return "", false, ErrInternalErrorOnFileUpload
	}
}

func (protonDrive *ProtonDrive) createFileUploadDraft(ctx context.Context, parentLink *proton.Link, filename string, modTime time.Time, mimeType string) (string, string, *crypto.SessionKey, *crypto.KeyRing, error) {
	parentNodeKR, err := protonDrive.getLinkKR(ctx, parentLink)
	if err != nil {
		return "", "", nil, nil, err
	}

	/*
		Encryption: parent link's node key
		Signature: share's signature address keys
	*/
	// New files get a v4 node key and the pre-crypto-refresh content format:
	// the official Proton clients (web included) cannot yet decrypt files that
	// originate in the crypto-refresh format, whether or not the auxiliary
	// fields stay non-AEAD. Revisions of files that already have a v6 node key
	// keep their format: the stored content key's v6 flag selects the v2 SEIPD
	// block encryption (see the block encryption below).
	newNodeKey, newNodePassphraseEnc, newNodePassphraseSignature, err := generateNodeKeys(parentNodeKR, protonDrive.DefaultAddrKR, false)
	if err != nil {
		return "", "", nil, nil, err
	}

	createFileReq := proton.CreateFileReq{
		ParentLinkID: parentLink.LinkID,

		// Name     string // Encrypted File Name
		// Hash     string // Encrypted File Name hash
		MIMEType: mimeType, // MIME Type

		// ContentKeyPacket          string // The block's key packet, encrypted with the node key.
		// ContentKeyPacketSignature string // Unencrypted signature of the content session key, signed with the NodeKey

		NodeKey:                 newNodeKey,                 // The private NodeKey, used to decrypt any file/folder content.
		NodePassphrase:          newNodePassphraseEnc,       // The passphrase used to unlock the NodeKey, encrypted by the owning Link/Share keyring.
		NodePassphraseSignature: newNodePassphraseSignature, // The signature of the NodePassphrase

		SignatureAddress: protonDrive.signatureAddress, // Signature email address used to sign passphrase and name
	}

	/*
		Encryption: parent link's node key
		Signature: share's signature address keys
	*/
	err = createFileReq.SetName(filename, protonDrive.DefaultAddrKR, parentNodeKR)
	if err != nil {
		return "", "", nil, nil, err
	}

	/*
		Encryption: parent link's node key
		Signature: parent link's node key
	*/
	signatureVerificationKR, err := protonDrive.getSignatureVerificationKeyring([]string{parentLink.SignatureEmail}, parentNodeKR)
	if err != nil {
		return "", "", nil, nil, err
	}
	parentHashKey, err := parentLink.GetHashKey(parentNodeKR, signatureVerificationKR)
	if err != nil {
		return "", "", nil, nil, err
	}

	/* Use parent's hash key */
	err = createFileReq.SetHash(filename, parentHashKey)
	if err != nil {
		return "", "", nil, nil, err
	}

	/*
		Encryption: parent link's node key
		Signature: share's signature address keys
	*/
	newNodeKR, err := getKeyRing(parentNodeKR, protonDrive.DefaultAddrKR, newNodeKey, newNodePassphraseEnc, newNodePassphraseSignature)
	if err != nil {
		return "", "", nil, nil, err
	}

	/*
		Encryption: current link's node key
		Signature: share's signature address keys
	*/
	newSessionKey, err := createFileReq.SetContentKeyPacketAndSignature(newNodeKR)
	if err != nil {
		return "", "", nil, nil, err
	}

	createFileAction := func() (*proton.CreateFileRes, *proton.Link, error) {
		createFileResp, err := protonDrive.c.CreateFile(ctx, protonDrive.MainShare.ShareID, createFileReq)
		if err != nil {
			// FIXME: check for duplicated filename by relying on checkAvailableHashes -> able to retrieve linkID too
			// Also saving generating resources such as new nodeKR, etc.

			if err != proton.ErrFileNameExist {
				// other real error caught
				return nil, nil, err
			}

			// search for the link within this folder which has an active/draft revision as we have a file creation conflict
			link, err := protonDrive.SearchByNameInActiveFolder(ctx, parentLink, filename, true, false, proton.LinkStateActive)
			if err != nil {
				return nil, nil, err
			}

			if link == nil {
				link, err = protonDrive.SearchByNameInActiveFolder(ctx, parentLink, filename, true, false, proton.LinkStateDraft)
				if err != nil {
					return nil, nil, err
				}

				if link == nil {
					// we have a real problem here (unless the assumption is wrong)
					// since we can't create a new file AND we can't locate a file with active/draft revision in it
					return nil, nil, ErrCantLocateRevision
				}
			}

			return nil, link, nil
		}

		return &createFileResp, nil, nil
	}

	createFileResp, link, err := createFileAction()
	if err != nil {
		return "", "", nil, nil, err
	}

	revisionID, shouldSubmitCreateFileRequestAgain, err := protonDrive.handleRevisionConflict(ctx, link, createFileResp)
	if err != nil {
		return "", "", nil, nil, err
	}

	if shouldSubmitCreateFileRequestAgain {
		// the case where the link has only a draft but no active revision
		// we need to delete the link and recreate one
		createFileResp, link, err = createFileAction()
		if err != nil {
			return "", "", nil, nil, err
		}

		revisionID, _, err = protonDrive.handleRevisionConflict(ctx, link, createFileResp)
		if err != nil {
			return "", "", nil, nil, err
		}
	}

	linkID := ""
	if link != nil {
		linkID = link.LinkID

		// get original sessionKey and nodeKR for the current link
		parentNodeKR, err = protonDrive.getLinkKRByID(ctx, link.ParentLinkID)
		if err != nil {
			return "", "", nil, nil, err
		}
		signatureVerificationKR, err := protonDrive.getSignatureVerificationKeyring([]string{link.SignatureEmail})
		if err != nil {
			return "", "", nil, nil, err
		}
		newNodeKR, err = link.GetKeyRing(parentNodeKR, signatureVerificationKR)
		if err != nil {
			return "", "", nil, nil, err
		}
		newSessionKey, err = link.GetSessionKey(newNodeKR)
		if err != nil {
			return "", "", nil, nil, err
		}
	} else {
		linkID = createFileResp.ID
	}

	return linkID, revisionID, newSessionKey, newNodeKR, nil
}

func (protonDrive *ProtonDrive) uploadAndCollectBlockData(ctx context.Context, newSessionKey *crypto.SessionKey, newNodeKR *crypto.KeyRing, file io.Reader, linkID, revisionID string) ([]byte, int64, []int64, string, error) {
	if newSessionKey == nil || newNodeKR == nil {
		return nil, 0, nil, "", ErrMissingInputUploadAndCollectBlockData
	}

	totalFileSize := int64(0)

	pendingUploadBlocks := make([]pendingUploadBlock, 0)
	manifestSignatureData := make([]byte, 0)
	uploadPendingBlocks := func() error {
		if len(pendingUploadBlocks) == 0 {
			return nil
		}

		requestLinks := func(ctx context.Context, blockList []proton.BlockUploadInfo) ([]proton.BlockUploadLink, error) {
			return protonDrive.c.RequestBlockUpload(ctx, proton.BlockUploadReq{
				AddressID:  protonDrive.MainShare.AddressID,
				ShareID:    protonDrive.MainShare.ShareID,
				LinkID:     linkID,
				RevisionID: revisionID,
				BlockList:  blockList,
			})
		}
		uploadBlock := func(ctx context.Context, link proton.BlockUploadLink, block []byte) error {
			if err := protonDrive.blockUploadSemaphore.Acquire(ctx, 1); err != nil {
				return err
			}
			defer protonDrive.blockUploadSemaphore.Release(1)

			return protonDrive.c.UploadBlock(ctx, link.BareURL, link.Token, bytes.NewReader(block))
		}
		if err := uploadBlockBatchWithRetry(
			ctx,
			pendingUploadBlocks,
			blockUploadMaxAttempts,
			requestLinks,
			uploadBlock,
			waitForBlockUploadRetry,
			protonDrive.Config.GetLogger(),
		); err != nil {
			return err
		}

		pendingUploadBlocks = pendingUploadBlocks[:0]

		return nil
	}

	shouldContinue := true
	sha1Digests := sha1.New()
	blockSizes := make([]int64, 0)
	for i := 1; shouldContinue; i++ {
		if (i-1) > 0 && (i-1)%UPLOAD_BATCH_BLOCK_SIZE == 0 {
			err := uploadPendingBlocks()
			if err != nil {
				return nil, 0, nil, "", err
			}
		}

		// read at most data of size UPLOAD_BLOCK_SIZE
		// for some reason, .Read might not actually read up to buffer size -> use io.ReadFull
		data := make([]byte, UPLOAD_BLOCK_SIZE) // FIXME: get block size from the server config instead of hardcoding it
		readBytes, err := io.ReadFull(file, data)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// might still have data to read!
				if readBytes == 0 {
					break
				}
				shouldContinue = false
			} else {
				// all other errors
				return nil, 0, nil, "", err
			}
		}
		data = data[:readBytes]
		totalFileSize += int64(readBytes)
		sha1Digests.Write(data)
		blockSizes = append(blockSizes, int64(readBytes))

		// encrypt block data
		/*
			Encryption: current link's session key
			Signature: share's signature address keys
		*/
		pgp := crypto.PGP()

		// Block data is encrypted with the crypto-refresh handle. When
		// newSessionKey carries the v6 flag (new files, and revisions of
		// crypto-refresh files) this produces a v2 SEIPD packet using
		// AES-256-GCM; for an older file's v4 content key it produces a v1
		// SEIPD, keeping the revision consistent with the original format.
		sessionEnc, err := protonDrivePGP().Encryption().SessionKey(newSessionKey).New()
		if err != nil {
			return nil, 0, nil, "", err
		}
		encMsg, err := sessionEnc.Encrypt(data)
		if err != nil {
			return nil, 0, nil, "", err
		}
		encData := encMsg.Bytes()

		// v2's SignDetachedEncrypted: sign the data with addrKR, then encrypt
		// the signature to newNodeKR. v3 does not expose a one-shot helper, so
		// we sign first and encrypt the signature bytes separately. Unlike the
		// block data, the signature must stay in the pre-crypto-refresh format
		// even though the file node key is v6 (see EncryptMessageNonAead).
		signHandle, err := pgp.Sign().SigningKeys(protonDrive.DefaultAddrKR).Detached().New()
		if err != nil {
			return nil, 0, nil, "", err
		}
		rawSig, err := signHandle.Sign(data, crypto.Bytes)
		if err != nil {
			return nil, 0, nil, "", err
		}
		encSignature, err := proton.EncryptMessageNonAead(newNodeKR, rawSig, nil)
		if err != nil {
			return nil, 0, nil, "", err
		}
		encSignatureStr, err := encSignature.Armor()
		if err != nil {
			return nil, 0, nil, "", err
		}

		h := sha256.New()
		h.Write(encData)
		hash := h.Sum(nil)
		base64Hash := base64.StdEncoding.EncodeToString(hash)
		if err != nil {
			return nil, 0, nil, "", err
		}
		manifestSignatureData = append(manifestSignatureData, hash...)

		pendingUploadBlocks = append(pendingUploadBlocks, pendingUploadBlock{
			blockUploadInfo: proton.BlockUploadInfo{
				Index:        i, // iOS drive: BE starts with 1
				Size:         int64(len(encData)),
				EncSignature: encSignatureStr,
				Hash:         base64Hash,
			},
			encData: encData,
		})
	}
	err := uploadPendingBlocks()
	if err != nil {
		return nil, 0, nil, "", err
	}

	sha1Hash := sha1Digests.Sum(nil)
	sha1String := hex.EncodeToString(sha1Hash)
	return manifestSignatureData, totalFileSize, blockSizes, sha1String, nil
}

func (protonDrive *ProtonDrive) commitNewRevision(ctx context.Context, nodeKR *crypto.KeyRing, xAttrCommon *proton.RevisionXAttrCommon, manifestSignatureData []byte, linkID, revisionID string) error {
	signHandle, err := crypto.PGP().Sign().SigningKeys(protonDrive.DefaultAddrKR).Detached().New()
	if err != nil {
		return err
	}
	manifestSig, err := signHandle.Sign(manifestSignatureData, crypto.Armor)
	if err != nil {
		return err
	}
	manifestSignatureString := string(manifestSig)

	commitRevisionReq := proton.CommitRevisionReq{
		ManifestSignature: manifestSignatureString,
		SignatureAddress:  protonDrive.signatureAddress,
	}

	err = commitRevisionReq.SetEncXAttrString(protonDrive.DefaultAddrKR, nodeKR, xAttrCommon)
	if err != nil {
		return err
	}

	err = protonDrive.c.CommitRevision(ctx, protonDrive.MainShare.ShareID, linkID, revisionID, commitRevisionReq)
	if err != nil {
		return err
	}

	return nil
}

// testParam is for integration test only
// 0 = normal mode
// 1 = up to create revision
// 2 = up to block upload
func (protonDrive *ProtonDrive) uploadFile(ctx context.Context, parentLink *proton.Link, filename string, modTime time.Time, file io.Reader, testParam int) (string, *proton.RevisionXAttrCommon, error) {
	// TODO: if we should use github.com/gabriel-vasile/mimetype to detect the MIME type from the file content itself
	// Note: this approach might cause the upload progress to display the "fake" progress, since we read in all the content all-at-once
	// mimetype.SetLimit(0)
	// mType := mimetype.Detect(fileContent)
	// mimeType := mType.String()

	// detect MIME type by looking at the filename only
	mimeType := mime.TypeByExtension(filepath.Ext(filename))
	if mimeType == "" {
		// api requires a mime type passed in
		mimeType = "text/plain"
	}

	/* step 1: create a draft */
	linkID, revisionID, newSessionKey, newNodeKR, err := protonDrive.createFileUploadDraft(ctx, parentLink, filename, modTime, mimeType)
	if err != nil {
		return "", nil, err
	}

	if testParam == 1 {
		return "", nil, nil
	}

	/* step 2: upload blocks and collect block data */
	manifestSignature, fileSize, blockSizes, digests, err := protonDrive.uploadAndCollectBlockData(ctx, newSessionKey, newNodeKR, file, linkID, revisionID)
	if err != nil {
		return "", nil, err
	}

	if testParam == 2 {
		// for integration tests
		// we try to simulate blocks uploaded but not yet commited
		return "", nil, nil
	}

	/* step 3: mark the file as active by commiting the revision */
	xAttrCommon := &proton.RevisionXAttrCommon{
		ModificationTime: modTime.Format("2006-01-02T15:04:05-0700"), /* ISO8601 */
		Size:             fileSize,
		BlockSizes:       blockSizes,
		Digests: map[string]string{
			"SHA1": digests,
		},
	}
	err = protonDrive.commitNewRevision(ctx, newNodeKR, xAttrCommon, manifestSignature, linkID, revisionID)
	if err != nil {
		return "", nil, err
	}

	return linkID, xAttrCommon, nil
}

func (protonDrive *ProtonDrive) UploadFileByReader(ctx context.Context, parentLinkID string, filename string, modTime time.Time, file io.Reader, testParam int) (string, *proton.RevisionXAttrCommon, error) {
	parentLink, err := protonDrive.getLink(ctx, parentLinkID)
	if err != nil {
		return "", nil, err
	}

	return protonDrive.uploadFile(ctx, parentLink, filename, modTime, file, testParam)
}

func (protonDrive *ProtonDrive) UploadFileByPath(ctx context.Context, parentLink *proton.Link, filename string, filePath string, testParam int) (linkID string, revisionXAttrCommon *proton.RevisionXAttrCommon, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", nil, err
	}
	defer utility.CheckClose(f, &err)

	info, err := os.Stat(filePath)
	if err != nil {
		return "", nil, err
	}

	in := bufio.NewReader(f)

	return protonDrive.uploadFile(ctx, parentLink, filename, info.ModTime(), in, testParam)
}

/*
There is a route that proton-go-api doesn't have - checkAvailableHashes.
This is used to quickly find the next available filename when the originally supplied filename is taken in the current folder.

Based on the code below, which is taken from the Proton iOS Drive app, we can infer that:
- when a file is to be uploaded && there is filename conflict after the first upload:
	- on web, user will be prompted with a) overwrite b) keep both by appending filename with iteration number c) do nothing
- on the iOS client logic, we can see that when the filename conflict happens (after the upload attampt failed)
	- the filename will be hashed by using filename + iteration
	- 10 iterations will be done per batch, each iteration's hash will be sent to the server
	- the server will return available hashes, and the client will take the lowest iteration as the filename to be used
	- will be used to search for the next available filename (using hashes avoids the filename being known to the server)
*/
