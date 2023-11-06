package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"flag"

	"github.com/gofrs/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"

	"github.com/joho/godotenv"
	"github.com/schollz/progressbar/v3"
)

var Info = log.New(os.Stdout, "\u001b[34mINFO: \u001B[0m", log.LstdFlags|log.Lshortfile)

var Warning = log.New(os.Stdout, "\u001b[33mWARNING: \u001B[0m", log.LstdFlags|log.Lshortfile)

var Error = log.New(os.Stdout, "\u001b[31mERROR: \u001b[0m", log.LstdFlags|log.Lshortfile)

var Debug = log.New(os.Stdout, "\u001b[36mDEBUG: \u001B[0m", log.LstdFlags|log.Lshortfile)

type Config struct {
	ApiURL        string        `envconfig:"API_URL" required:"true"`
	SessionToken  string        `envconfig:"SESSION_TOKEN" required:"true"`
	PartSize      fs.SizeSuffix `envconfig:"PART_SIZE"`
	Workers       int           `envconfig:"WORKERS" default:"4"`
	RandomisePart bool          `envconfig:"RANDOMISE_PART" default:"true"`
	ChannelID     int64         `envconfig:"CHANNEL_ID"`
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

type MetadataRequestOptions struct {
	PerPage       uint64
	SearchField   string
	Search        string
	NextPageToken string
}

type FileInfo struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Size     int64  `json:"size"`
	ParentId string `json:"parentId"`
	Type     string `json:"type"`
	ModTime  string `json:"updatedAt"`
}

type ReadMetadataResponse struct {
	Files         []FileInfo `json:"results"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}

type Uploader struct {
	http       *rest.Client
	numWorkers int
	partSize   int64
	channelID  int64
	pacer      *fs.Pacer
	ctx        context.Context
}

var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func loadConfigFromEnv() (*Config, error) {

	var config Config

	err := godotenv.Load("upload.env")
	if err != nil {
		return nil, err
	}

	err = envconfig.Process("", &config)
	if err != nil {
		panic(err)
	}
	if config.PartSize == 0 {
		config.PartSize = 1000 * fs.Mebi
	}

	return &config, nil
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

func (u *Uploader) uploadFile(filePath string, destDir string, randomisePart bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, 512)
	_, err = file.Read(buffer)
	if err != nil {
		Error.Println("Error reading file:", filePath, err)
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

	numParts := fileSize / u.partSize
	if fileSize%u.partSize != 0 {
		numParts++
	}

	uploadedParts := make(chan UploadPartOut, numParts)
	concurrentWorkers := make(chan struct{}, u.numWorkers)

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
		start := i * u.partSize
		end := start + u.partSize
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
				Error.Println("Error:", err)
				return
			}
			defer partFile.Close()

			_, err = partFile.Seek(start, io.SeekStart)

			if err != nil {
				Error.Println("Error:", err)
				return
			}

			name := fileName

			if randomisePart {
				u1, _ := uuid.NewV4()
				name = hex.EncodeToString(u1.Bytes())
			} else if numParts > 1 {
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
				Parameters: url.Values{
					"fileName":   []string{name},
					"partNo":     []string{strconv.FormatInt(partNumber+1, 10)},
					"totalparts": []string{strconv.FormatInt(int64(numParts), 10)},
					"channelId":  []string{strconv.FormatInt(int64(u.channelID), 10)},
				},
			}

			var part UploadPartOut
			resp, err := u.http.CallJSON(context.TODO(), &opts, nil, &part)

			if err != nil {
				Error.Println("Error:", err)
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
		return fmt.Errorf("upload failed: %s", fileName)
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
		ChannelID: u.channelID,
	}

	json.Marshal(filePayload)

	if err != nil {
		return err
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files",
	}

	err = u.pacer.Call(func() (bool, error) {
		resp, err := u.http.CallJSON(u.ctx, &opts, &filePayload, nil)
		return shouldRetry(u.ctx, resp, err)
	})

	if err != nil {
		return err
	}

	err = u.pacer.Call(func() (bool, error) {
		resp, err := u.http.CallJSON(u.ctx, &rest.Opts{Method: "DELETE", Path: uploadURL}, nil, nil)
		return shouldRetry(u.ctx, resp, err)
	})

	if err != nil {
		return err
	}

	return nil
}

func (u *Uploader) createRemoteDir(path string) error {
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

	err := u.pacer.Call(func() (bool, error) {
		resp, err := u.http.CallJSON(u.ctx, &opts, &mkdir, nil)
		return shouldRetry(u.ctx, resp, err)
	})

	if err != nil {
		return err
	}
	return nil
}

func (u *Uploader) readMetaDataForPath(path string, options *MetadataRequestOptions) (*ReadMetadataResponse, error) {

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"path":          []string{path},
			"perPage":       []string{strconv.FormatUint(options.PerPage, 10)},
			"sort":          []string{"name"},
			"order":         []string{"asc"},
			"op":            []string{"list"},
			"nextPageToken": []string{options.NextPageToken},
		},
	}
	var err error
	var info ReadMetadataResponse
	var resp *http.Response

	err = u.pacer.Call(func() (bool, error) {
		resp, err = u.http.CallJSON(u.ctx, &opts, nil, &info)
		return shouldRetry(u.ctx, resp, err)
	})

	if err != nil && resp.StatusCode == 404 {
		return nil, fs.ErrorDirNotFound
	}

	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (u *Uploader) list(path string) (files []FileInfo, err error) {

	var limit uint64 = 500
	var nextPageToken string = ""
	for {
		opts := &MetadataRequestOptions{
			PerPage:       limit,
			NextPageToken: nextPageToken,
		}

		info, err := u.readMetaDataForPath(path, opts)
		if err != nil {
			return nil, err
		}

		files = append(files, info.Files...)

		nextPageToken = info.NextPageToken
		if nextPageToken == "" {
			break
		}
	}
	return files, nil
}

func (u *Uploader) checkFileExists(name string, files []FileInfo) bool {
	for _, item := range files {
		if item.Name == name {
			return true
		}
	}
	return false
}

func (u *Uploader) uploadFilesInDirectory(sourcePath string, destDir string, randomisePart bool) error {
	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return err
	}

	destDir = strings.ReplaceAll(destDir, "\\", "/")

	files, err := u.list(destDir)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		fullPath := filepath.Join(sourcePath, entry.Name())

		if entry.IsDir() {
			subDir := filepath.Join(destDir, entry.Name())
			subDir = strings.ReplaceAll(subDir, "\\", "/")
			err := u.createRemoteDir(subDir)
			if err != nil {
				Error.Fatalln(err)
			}
			err = u.uploadFilesInDirectory(fullPath, subDir, randomisePart)
			if err != nil {
				Error.Println(err)
			}
		} else {

			exists := u.checkFileExists(entry.Name(), files)
			if !exists {
				err := u.uploadFile(fullPath, destDir, randomisePart)
				if err != nil {
					Error.Println("upload failed:", entry.Name(), err)
				}
			} else {
				Info.Println("file exists:", entry.Name())
			}
		}
	}
	return nil
}

func main() {
	sourcePath := flag.String("path", "", "File or directory path to upload")
	destDir := flag.String("dest", "", "Remote directory for uploaded files")
	flag.Parse()

	if *sourcePath == "" || *destDir == "" {
		fmt.Println("Usage: ./uploader -path <file_or_directory_path> -dest <remote_directory>")
		return
	}

	config, err := loadConfigFromEnv()

	if err != nil {
		Error.Fatalln(err)
	}

	authCookie := &http.Cookie{
		Name:  "user-session",
		Value: config.SessionToken,
	}

	ctx := context.Background()

	httpClient := rest.NewClient(http.DefaultClient).SetRoot(config.ApiURL).SetCookie(authCookie)

	pacer := fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(400*time.Millisecond),
		pacer.MaxSleep(5*time.Second), pacer.DecayConstant(2), pacer.AttackConstant(0)))

	uploader := &Uploader{
		http:       httpClient,
		numWorkers: config.Workers,
		channelID:  config.ChannelID,
		partSize:   int64(config.PartSize),
		pacer:      pacer,
		ctx:        ctx,
	}

	err = uploader.createRemoteDir(*destDir)

	if err != nil {
		Error.Fatalln(err)
	}

	if fileInfo, err := os.Stat(*sourcePath); err == nil {
		if fileInfo.IsDir() {
			err := uploader.uploadFilesInDirectory(*sourcePath, *destDir, config.RandomisePart)
			if err != nil {
				Error.Println("upload failed:", err)
			}
		} else {
			if err := uploader.uploadFile(*sourcePath, *destDir, config.RandomisePart); err != nil {
				Error.Println("upload failed:", err)
			}
		}
	} else {
		Error.Fatalln(err)
	}

	Info.Println("Uploads complete!")
}
