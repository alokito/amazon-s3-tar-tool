// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func buildToc(ctx context.Context, objectList []*S3Obj, timeResolution TimeResolutionValue) (*S3Obj, *S3Obj, error) {

	headers := processHeaders(ctx, objectList, false, timeResolution)
	toc, err := _buildToc(ctx, headers, objectList)
	if err != nil {
		return nil, nil, err
	}

	// Build a header with the original data
	tocObj := NewS3Obj()
	tocObj.Key = aws.String("toc.csv")
	tocObj.AddData(toc.Bytes())
	// passing nil as we don't need to set permissions/owner/group for toc.csv
	tocHeader := buildHeader(tocObj, nil, false, nil, timeResolution)
	tocHeader.Bucket = objectList[0].Bucket
	tocObj.Bucket = objectList[0].Bucket

	return tocObj, &tocHeader, nil
}

func _buildToc(ctx context.Context, headers []*S3Obj, objectList []*S3Obj) (*bytes.Buffer, error) {

	var currLocation int64 = 0
	data, err := createCSVTOC(currLocation, headers, objectList)
	if err != nil {
		return nil, err
	}
	estimate := int64(data.Len())

	for {
		data, err = createCSVTOC(int64(estimate), headers, objectList)
		if err != nil {
			return nil, err
		}
		l := int64(data.Len())
		lp := l + findPadding(l)
		if lp >= estimate {
			break
		} else {
			estimate = lp
		}
	}

	return data, nil
}

func createCSVTOC(offset int64, headers []*S3Obj, objectList []*S3Obj) (*bytes.Buffer, error) {
	headerOffset := paxTarHeaderSize
	if tarFormat == tar.FormatGNU {
		headerOffset = gnuTarHeaderSize
	}
	var currLocation int64 = offset + headerOffset
	currLocation = currLocation + findPadding(currLocation)
	buf := bytes.Buffer{}
	toc := [][]string{}

	for i := 0; i < len(objectList); i++ {
		currLocation += *headers[i].Size
		line := []string{}
		line = append(line,
			*objectList[i].Key,
			fmt.Sprintf("%d", currLocation),
			fmt.Sprintf("%d", *objectList[i].Size),
			*objectList[i].ETag)
		toc = append(toc, line)
		currLocation += *objectList[i].Size
	}
	cw := csv.NewWriter(&buf)
	if err := cw.WriteAll(toc); err != nil {
		return nil, err
	}
	cw.Flush()

	return &buf, nil
}

func buildFirstPart(csvData []byte) *S3Obj {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)
	hdr := &tar.Header{
		Name:       "toc.csv",
		Mode:       0600,
		Size:       int64(len(csvData)),
		ModTime:    time.Now(),
		ChangeTime: time.Now(),
		AccessTime: time.Now(),
		Format:     tarFormat,
	}
	buf.Write(pad)
	if err := tw.WriteHeader(hdr); err != nil {
		log.Fatal(err)
	}
	if err := tw.Flush(); err != nil {
		// we ignore this error, the tar library will complain that we
		// didn't write the whole file. This part is already on Amazon S3
	}
	buf.Write(csvData)

	padding := findPadding(int64(len(csvData)))
	if padding == 0 {
		padding = blockSize
	}
	lastBytes := make([]byte, padding)
	buf.Write(lastBytes)

	endPadding := NewS3Obj()
	endPadding.AddData(buf.Bytes())
	return endPadding
}

func tryParseHeader(ctx context.Context, svc *s3.Client, opts *S3TarS3Options, start int64) (*tar.Header, int64, error) {
	var i int64 = 512
	var windowStart int64 = start
	var header *tar.Header
	var offset int64 = 0
	data := make([]byte, blockSize*10)

	for ; i < (512 * 10); windowStart, i = windowStart+blockSize, i+blockSize {
		Debugf(ctx, "trying to parse header from %d-%d\n", start, start+i)
		Debugf(ctx, "downloading from %d-%d\n", windowStart, windowStart+blockSize)
		r, err := getObjectRange(ctx, svc, opts.SrcBucket, opts.SrcKey, windowStart, windowStart+blockSize-1)
		if err != nil {
			panic(err)
		}
		defer r.Close()

		all, _ := io.ReadAll(r)
		copy(data[i-blockSize:i], all)

		if i == blockSize*2 {
			endBlock := make([]byte, blockSize*2)
			if bytes.Compare(endBlock, data[0:1024]) == 0 {
				return nil, offset, io.EOF
			}
		}

		nr := bytes.NewReader(data[0:i])
		tr := tar.NewReader(nr)
		h, err := tr.Next()
		if err == nil {
			header = h
			offset = start + i
			break
		}
	}
	return header, offset, nil
}

// GenerateToc creates a TOC csv of an existing TAR file (not created by s3tar)
// tar file MUST NOT have compression.
// tar file must be on the local file system to.
func GenerateToc(ctx context.Context, svc *s3.Client, tarFile, outputToc string, opts *S3TarS3Options) error {

	if strings.Contains(tarFile, "s3://") {
		// remote file on s3
		fmt.Printf("file is on s3")

		w, err := os.Create(outputToc)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer w.Close()
		cw := csv.NewWriter(w)

		var start int64 = 0
		for {

			header, offset, err := tryParseHeader(ctx, svc, opts, start)
			if err == io.EOF {
				Debugf(ctx, "reached EOF")
				break
			}
			if err != nil {
				// log something
				break
			}

			offsetStr := fmt.Sprintf("%d", offset)
			size := fmt.Sprintf("%d", header.Size)
			record := []string{header.Name, offsetStr, size, ""}
			if err = cw.Write(record); err != nil {
				return err
			}

			start = offset + header.Size + findPadding(offset+header.Size)
			Debugf(ctx, "next start: %d\n", start)
		}
		cw.Flush()

		return nil
	} else {
		// local file
		fmt.Printf("file is local")

		r, err := os.Open(tarFile)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer r.Close()

		w, err := os.Create(outputToc)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer w.Close()

		cw := csv.NewWriter(w)
		tr := tar.NewReader(r)
		for {
			h, err := tr.Next()
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}

			offset, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

			offsetStr := fmt.Sprintf("%d", offset)
			size := fmt.Sprintf("%d", h.Size)
			record := []string{h.Name, offsetStr, size, ""}
			if err = cw.Write(record); err != nil {
				return err
			}

		}
		cw.Flush()
		return nil
	}
}
