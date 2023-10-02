package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"flag"

	"github.com/rclone/rclone/lib/rest"

	"github.com/joho/godotenv"
	"github.com/schollz/progressbar/v3"
)

var sizeRegex = regexp.MustCompile(`(?i)^(\d+(\.\d+)?)\s*([KMGT]B|bytes?)$`)

type Config struct {
	ApiURL       string
	SessionToken string
	PartSize     int64
	Workers      int
}

type UploadPartOut struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PartId     int    `json:"partId"`
	PartNo     int    `json:"partNo"`
	TotalParts int    `json:"totalParts"`
	ChannelID  int64  `json:"channelId"`
	Size       int64  `json:"size"`
}

type Part struct {
	ID     int64 `json:"id"`
	PartNo int   `json:"partNo"`
}

// Add a new field for Telegram channel ID in the FilePayload struct
type FilePayload struct {
    Name      string `json:"name"`
    Type      string `json:"type"`
    Parts     []Part `json:"parts,omitempty"`
    MimeType  string `json:"mimeType"`
    Path      string `json:"path"`
    Size      int64  `json:"size"`
    ChannelID int64  `json:"channelId"`
}

type CreateDirRequest struct {
	Path string `json:"path"`
}

func fileSizeToBytes(sizeStr string) (int64, error) {
	match := sizeRegex.FindStringSubmatch(strings.ToLower(sizeStr))
	if len(match) != 4 {
		return 0, fmt.Errorf("invalid format: %s", sizeStr)
	}

	size, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, err
	}

	unit := match[3]
	switch unit {
	case "kb", "kilobyte", "kilobytes":
		return int64(size * 1024), nil
	case "mb", "megabyte", "megabytes":
		return int64(size * 1024 * 1024), nil
	case "gb", "gigabyte", "gigabytes":
		return int64(size * 1000 * 1024 * 1024), nil
	default:
		return 0, fmt.Errorf("unsupported unit: %s", unit)
	}
}

func loadConfigFromEnv() (*Config, error) {
	err := godotenv.Load("upload.env")
	if err != nil {
		return nil, err
	}

	partSize := "1GB"

	if os.Getenv("PART_SIZE") != "" {
		partSize = os.Getenv("PART_SIZE")
	}

	partSizeBytes, err := fileSizeToBytes(partSize)
	if err != nil {
		return nil, err
	}

	workers := 4

	if os.Getenv("WORKERS") != "" {
		workers, _ = strconv.Atoi(os.Getenv("WORKERS"))
	}

	config := &Config{
		ApiURL:       os.Getenv("API_URL"),
		SessionToken: os.Getenv("SESSION_TOKEN"),
		PartSize:     partSizeBytes,
		Workers:      workers,
	}

	return config, nil
}

type ProgressReader struct {
	io.Reader
	Reporter func(r int64)
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	pr.Reporter(int64(n))
	return
}

func uploadFile(httpClient *rest.Client, filePath string, destDir string, partSize int64, numWorkers int, channelID int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, 512)
	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return nil
	}

	mimeType := http.DetectContentType(buffer)

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	fileName := filepath.Base(filePath)
	input := fmt.Sprintf("%s:%s:%d", fileName, destDir, fileSize)

	hash := md5.Sum([]byte(input))
	hashString := hex.EncodeToString(hash[:])

	uploadURL := fmt.Sprintf("/api/uploads/%s", hashString)

	var wg sync.WaitGroup

	numParts := fileSize / partSize
	if fileSize%partSize != 0 {
		numParts++
	}

	uploadedParts := make(chan UploadPartOut, numParts)
	concurrentWorkers := make(chan struct{}, numWorkers)

	bar := progressbar.NewOptions64(fileSize,
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionSetDescription(fileName),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true))

	go func() {
		wg.Wait()
		close(uploadedParts)
		bar.Finish()
		bar.Close()
	}()

	for i := int64(0); i < numParts; i++ {
		start := i * partSize
		end := start + partSize
		if end > fileSize {
			end = fileSize
		}

		concurrentWorkers <- struct{}{}
		wg.Add(1)

		go func(partNumber int64, start, end int64) {
			defer wg.Done()
			defer func() {
				<-concurrentWorkers
			}()

			partFile, err := os.Open(filePath)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer partFile.Close()

			_, err = partFile.Seek(start, io.SeekStart)

			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			name := fileName

			if numParts > 1 {
				name = fmt.Sprintf("%s.part.%03d", fileName, partNumber+1)
			}

			pr := &ProgressReader{partFile, func(r int64) {
				bar.Add64(r)
			}}

			contentLength := end - start
			reader := io.LimitReader(pr, contentLength)

			opts := rest.Opts{
				Method:        "POST",
				Path:          uploadURL,
				Body:          reader,
				ContentLength: &contentLength,
				ContentType:   "application/octet-stream",
				Parameters: url.Values{
					"fileName":   []string{name},
					"partNo":     []string{strconv.FormatInt(partNumber+1, 10)},
					"totalparts": []string{strconv.FormatInt(int64(numParts), 10)},
				},
			}

			var part UploadPartOut
			resp, err := httpClient.CallJSON(context.TODO(), &opts, nil, &part)

			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			if resp.StatusCode == 200 {
				uploadedParts <- part
			}
		}(i, start, end)
	}

	var parts []Part
	for uploadPart := range uploadedParts {
		parts = append(parts, Part{ID: int64(uploadPart.PartId), PartNo: uploadPart.PartNo})
	}

	if len(parts) != int(numParts) {
		return errors.New("upload failed")
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNo < parts[j].PartNo
	})

    filePayload := FilePayload{
        Name:      fileName,
        Type:      "file",
        Parts:     parts,
        MimeType:  mimeType,
        Path:      destDir,
        Size:      fileSize,
        ChannelID: channelID, // Set the Telegram channel ID
    }

	json.Marshal(filePayload)

	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return err
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files",
	}

	resp, err := httpClient.CallJSON(context.TODO(), &opts, &filePayload, nil)

	if resp.StatusCode != 200 {
		fmt.Println("Request failed with status code:", resp.StatusCode)
		return err
	}

	resp, err = httpClient.CallJSON(context.TODO(), &rest.Opts{Method: "DELETE", Path: uploadURL}, nil, nil)

	if resp.StatusCode != 200 {
		fmt.Println("Request failed with status code:", resp.StatusCode)
		return err
	}

	return nil
}

func createRemoteDir(httpClient *rest.Client, path string) error {
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/makedir",
	}

	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}

	mkdir := CreateDirRequest{
		Path: path,
	}

	_, err := httpClient.CallJSON(context.TODO(), &opts, &mkdir, nil)

	if err != nil {
		return err
	}
	return nil
}

func uploadFilesInDirectory(httpClient *rest.Client, sourcePath string, destDir string, partSize int64, numWorkers int) error {
	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		fullPath := filepath.Join(sourcePath, entry.Name())

		if entry.IsDir() {
			subDir := filepath.Join(destDir, entry.Name())
			subDir = strings.ReplaceAll(subDir, "\\", "/")
			err := createRemoteDir(httpClient, subDir)
			if err != nil {
				return err
			}
			err = uploadFilesInDirectory(httpClient, fullPath, subDir, partSize, numWorkers)
			if err != nil {
				return err
			}
		} else {
			err := uploadFile(httpClient, fullPath, strings.ReplaceAll(destDir, "\\", "/"), partSize, numWorkers)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
    sourcePath := flag.String("path", "", "File or directory path to upload")
    destDir := flag.String("dest", "", "Remote directory for uploaded files")
    channelID := flag.Int64("channelID", 0, "Telegram channel ID") // Add a new flag for channel ID
    flag.Parse()

    if *sourcePath == "" || *destDir == "" || *channelID == 0 {
        fmt.Println("Usage: ./uploader -path <file_or_directory_path> -dest <remote_directory> -channelID <telegram_channel_id>")
        return
    }

    config, err := loadConfigFromEnv()

    if err != nil {
        fmt.Println("Error:", err)
        return
    }

    authCookie := &http.Cookie{
        Name:  "user-session",
        Value: config.SessionToken,
    }

    httpClient := rest.NewClient(http.DefaultClient).SetRoot(config.ApiURL).SetCookie(authCookie)

    err = createRemoteDir(httpClient, *destDir)

    if err != nil {
        fmt.Println("Error:", err)
        return
    }

    if fileInfo, err := os.Stat(*sourcePath); err == nil {
        if fileInfo.IsDir() {
            err := uploadFilesInDirectory(httpClient, *sourcePath, *destDir, config.PartSize, config.Workers, *channelID) // Pass the channel ID
            if err != nil {
                fmt.Println("Error uploading files:", err)
            }
        } else {
            if err := uploadFile(httpClient, *sourcePath, *destDir, config.PartSize, config.Workers, *channelID); // Pass the channel ID
                err != nil {
                fmt.Println("Error uploading file:", err)
            }
        }
    } else {
        fmt.Println("Error:", err)
    }

    fmt.Println("Uploads complete!")
}
