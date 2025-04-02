package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

func createStubFile(ctx context.Context, beegfs filesystem.Provider, mappings *util.Mappings, store *beemsg.NodeStore, path string, remotePath string, target uint32, overwrite bool) error {
	rstUrl := []byte(fmt.Sprintf("rst://%d:%s", target, remotePath))
	if err := beegfs.CreateWriteClose(path, rstUrl, overwrite); err != nil {
		return err
	}

	return setStubFlag(ctx, mappings, store, path)
}

func verifyStubFileRstUrlMatches(beegfs filesystem.Provider, path string, remotePath string, rstId uint32) error {
	// Amazon s3 allows object key names to be up to 1024 bytes in length. Note that this is a
	// byte limit, so if your key contains multi-byte UTF-8 characters, the number of characters
	// may be fewer than 1024. The extra 0 bytes on the right will be trimmed.
	reader, _, err := beegfs.ReadFilePart(path, 0, 1024)
	if err != nil {
		return ErrStubNotReadable
	}

	rstUrl, err := io.ReadAll(reader)
	if err != nil {
		return ErrStubNotReadable
	}
	rstUrl = bytes.TrimRight(rstUrl, "\x00")

	if urlRstId, urlKey, err := parseRstUrl(rstUrl); err != nil {
		return ErrStubUrlMalformed
	} else if urlRstId != rstId {
		return fmt.Errorf("stub file's remote target does not match the request: %w", ErrStubUrlMismatch)
	} else if strings.TrimSpace(urlKey) != remotePath {
		return fmt.Errorf("stub file's remote path does not match the request: %w", ErrStubUrlMismatch)
	}

	return nil
}

func clearStubFlag(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, path string) error {
	return setFileDataState(ctx, mappings, store, path, beegfs.FileDataStateLocal)
}

func setStubFlag(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, path string) error {
	return setFileDataState(ctx, mappings, store, path, beegfs.FileDataStateOffloaded)
}

func setFileDataState(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, path string, state beegfs.FileDataState) error {
	entry, ownerNode, err := entry.GetEntryAndOwnerFromPath(ctx, mappings, path)
	if err != nil {
		return fmt.Errorf("unable to retrieve entry info: %w", err)
	}

	if entry.EntryType != beegfs.EntryRegularFile {
		return fmt.Errorf("unable to set stub flag status! File must be a regular file: %w", errors.ErrUnsupported)
	}

	request := &msg.SetFileDataStateRequest{EntryInfo: entry, DataState: state}
	resp := &msg.SetFileDataStateResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		return fmt.Errorf("unable to complete request to set stub flag status, %s: %w", path, err)
	}

	if resp.Result != beegfs.OpsErr_SUCCESS {
		return fmt.Errorf("server returned an error setting the stub flag status, %s: %w", path, resp.Result)
	}
	return nil
}

func isFileStubbed(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, path string) (bool, error) {
	entry, ownerNode, err := entry.GetEntryAndOwnerFromPath(ctx, mappings, path)
	if err != nil {
		return false, fmt.Errorf("unable to retrieve entry info: %w", err)
	}

	request := &msg.GetEntryInfoRequest{EntryInfo: entry}
	resp := &msg.GetEntryInfoResponse{}
	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		return true, fmt.Errorf("unable to complete entry's stub flag status request, %s: %w", path, err)
	}

	return resp.FileDataState == beegfs.FileDataStateOffloaded, nil
}

func parseRstUrl(url []byte) (uint32, string, error) {
	urlString := string(url)
	re := regexp.MustCompile(`^rst://([0-9]+):(.+)$`)
	matches := re.FindStringSubmatch(urlString)
	if len(matches) != 3 {
		return 0, "", fmt.Errorf("input does not match expected format: rst://<number>:<s3-key>")
	}

	num, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("failed to parse number: %w", err)
	}
	s3Key := matches[2]

	return uint32(num), s3Key, nil
}

// stripGlobPattern extracts the longest leading substring from the given pattern that contains
// no glob characters (e.g., '*', '?', or '['). This base prefix is used to efficiently list
// objects in an S3 bucket, while the original glob pattern is later applied to filter the results.
func stripGlobPattern(pattern string) string {
	globCharacters := "*?["
	position := 0
	for {
		index := strings.IndexAny(pattern[position:], globCharacters)
		if index == -1 {
			return pattern
		}
		candidate := position + index

		// Check for escape characters
		backslashCount := 0
		for i := candidate - 1; i >= 0 && pattern[i] == '\\'; i-- {
			backslashCount++
		}
		if backslashCount%2 == 0 {
			return pattern[:candidate]
		}

		// Check whether the last character was escaped
		position = candidate + 1
		if position >= len(pattern) {
			return pattern
		}
	}
}
