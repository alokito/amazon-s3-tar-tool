// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type RecursiveConcat struct {
	Client      *s3.Client
	Region      string
	EndpointUrl string
	Bucket      string
	DstPrefix   string
	DstKey      string
	block       S3Obj
}

type RecursiveConcatOptions struct {
	Client      *s3.Client
	Region      string
	EndpointUrl string
	Bucket      string
	DstPrefix   string
	DstKey      string
}

// type RecursiveConcatOption func(r *RecursiveConcat)

func (r *RecursiveConcat) CreateFirstBlock(ctx context.Context) {
	//randomize?
	key := filepath.Join(r.DstPrefix, r.DstKey+".parts", "min-size-block")
	now := time.Now()
	output, err := putObject(ctx, r.Client, r.Bucket, key, pad)
	if err != nil {
		Infof(ctx, err.Error())
		panic(err)
	}
	r.block = S3Obj{
		Bucket: r.Bucket,
		Object: types.Object{
			Key:          &key,
			Size:         aws.Int64(int64(len(pad))),
			LastModified: &now,
			ETag:         output.ETag,
		},
	}
}

func NewRecursiveConcat(ctx context.Context, options RecursiveConcatOptions, optFns ...func(*RecursiveConcatOptions)) (*RecursiveConcat, error) {

	options = options.Copy()

	checkRequiredArgs(&options)

	for _, fn := range optFns {
		fn(&options)
	}

	rc := &RecursiveConcat{
		Client:      options.Client,
		Region:      options.Region,
		EndpointUrl: options.EndpointUrl,
		Bucket:      options.Bucket,
		DstPrefix:   options.DstPrefix,
		DstKey:      options.DstKey,
	}
	rc.CreateFirstBlock(ctx)

	return rc, nil
}

func (r *RecursiveConcat) uploadPart(object *S3Obj, uploadId string, bucket, key string, partNum int32) (types.CompletedPart, error) {

	input := &s3.UploadPartInput{
		Bucket:     &bucket,
		Key:        &key,
		PartNumber: aws.Int32(partNum),
		UploadId:   &uploadId,
		Body:       io.ReadSeeker(bytes.NewReader(object.Data)),
	}

	res, err := r.Client.UploadPart(context.TODO(), input)
	if err != nil {
		return types.CompletedPart{}, err
	}
	return types.CompletedPart{
		ETag:       res.ETag,
		PartNumber: input.PartNumber}, nil
}

func (r *RecursiveConcat) uploadPartCopy(object *S3Obj, uploadId string, bucket, key string, partNum int32, start, end int64) (types.CompletedPart, error) {

	copySourceRange := fmt.Sprintf("bytes=%d-%d", start, end-1)

	var copySource = url.QueryEscape(object.Bucket + "/" + *object.Key)
	input := s3.UploadPartCopyInput{
		Bucket:          &bucket,
		Key:             &key,
		PartNumber:      aws.Int32(partNum),
		UploadId:        &uploadId,
		CopySource:      aws.String(copySource),
		CopySourceRange: aws.String(copySourceRange),
	}

	res, err := r.Client.UploadPartCopy(context.TODO(), &input)
	if err != nil {
		return types.CompletedPart{}, err
	}

	return types.CompletedPart{
		ETag:       res.CopyPartResult.ETag,
		PartNumber: input.PartNumber}, nil

}

func (r *RecursiveConcat) mergePair(ctx context.Context, objectList []*S3Obj, trim int64, bucket, key string) (*S3Obj, error) {
	complete := NewS3Obj()

	if len(objectList) > 2 {
		return nil, fmt.Errorf("mergePair needs two or less *S3Obj")
	}

	output, err := r.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		ACL:    types.ObjectCannedACLBucketOwnerFullControl,
	})
	if err != nil {
		return complete, err
	}

	uploadId := *output.UploadId

	Debugf(ctx, "CreateMultipartUpload(%s) to %s/%s", uploadId[:8], bucket, key)

	parts := []types.CompletedPart{}
	var accumSize int64 = 0
	for i, o := range objectList {
		part := types.CompletedPart{}
		var partNum int32 = int32(i + 1)
		var err error
		if len(o.Data) > 0 {
			Debugf(ctx, "UploadPart(%s) Start %s/%s %d", uploadId[:8], o.Bucket, *o.Key, len(o.Data))
			part, err = r.uploadPart(o, uploadId, bucket, key, partNum)
			accumSize += int64(len(o.Data))
			if err == nil {
				Debugf(ctx, "UploadPart(%s) Success source %s/%s (size %d) -> dest %s/%s", uploadId[:8], o.Bucket, *o.Key, len(o.Data), bucket, key)
			} else {
				fmt.Printf("error 1\n")
				fmt.Printf("UploadPart(%s) failed part: %d, source %s/%s (bytes %d) -> dest %s/%s, %s upload %s", uploadId[:8], partNum, o.Bucket, *o.Key, len(o.Data), bucket, key, err.Error(), uploadId)
			}
		} else if *o.Size > 0 {
			Debugf(ctx, "UploadPartCopy(%s) Start %s/%s %d", uploadId[:8], o.Bucket, *o.Key, *o.Size)
			part, err = r.uploadPartCopy(o, uploadId, bucket, key, partNum, trim, *o.Size)
			accumSize += int64(*o.Size) - trim
			if err == nil {
				Debugf(ctx, "UploadPartCopy(%s) Success source %s/%s (bytes %d-%d) -> dest %s/%s", uploadId[:8], o.Bucket, *o.Key, trim, *o.Size, bucket, key)
			} else {
				fmt.Printf("error 1\n")
				fmt.Printf("UploadPartCopy(%s) failed part: %d, source %s/%s (bytes %d-%d) -> dest %s/%s, %s upload %s", uploadId[:8], partNum, o.Bucket, *o.Key, trim, *o.Size, bucket, key, err.Error(), uploadId)
			}
		}
		if err != nil {
			return complete, err
		}
		if *o.Size > 0 {
			parts = append(parts, part)
		}
	}

	completeOutput, err := r.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return complete, err
	} else {
		Debugf(ctx, "CompleteMultipartUpload(%s) to %s/%s", uploadId[:8], bucket, key)
	}

	now := time.Now()
	complete = &S3Obj{
		Bucket: *completeOutput.Bucket,
		Object: types.Object{
			Key:          completeOutput.Key,
			ETag:         completeOutput.ETag,
			Size:         &accumSize,
			LastModified: &now,
		},
	}

	return complete, nil
}

func calculateFinalSize(objectList []*S3Obj) int64 {
	var accum int64 = 0
	for _, v := range objectList {
		accum += *v.Size
	}
	return accum
}

func (r *RecursiveConcat) ConcatObjects(ctx context.Context, objectList []*S3Obj, bucket, key string) (*S3Obj, error) {

	// if calculateFinalSize(objectList) < fileSizeMin+1 {
	// 	return &S3Obj{}, fmt.Errorf("Unable to concatenate these files, too small")
	// }

	if len(objectList) == 0 {
		return NewS3Obj(), fmt.Errorf("no elements passed to concat")
	}

	trimStart := false
	if *objectList[0].Size < fileSizeMin {
		objectList = append([]*S3Obj{&r.block}, objectList...)
		trimStart = true
	}

	accum := objectList[0]
	for _, object := range objectList[1:] {
		if object.Bucket == "" {
			object.Bucket = bucket
		}
		var err error
		Debugf(ctx, "accum: s3://%s/%s <- s3://%s/%s data %d, size %d", accum.Bucket, *accum.Key, object.Bucket, *object.Key, len(object.Data), *object.Size)
		accum, err = r.mergePair(ctx, []*S3Obj{accum, object}, 0, bucket, key)
		if err != nil {
			return nil, err
		}
	}

	// sort.Slice(parts, func(i, j int) bool {
	// 	return parts[i].PartNumber < parts[j].PartNumber
	// })
	// fmt.Printf("%+v", completeOutput)

	if trimStart {
		var err error
		accum, err = r.mergePair(ctx, []*S3Obj{accum}, fileSizeMin, bucket, key)
		if err != nil {
			Debugf(ctx, "error 2\n%s %s", bucket, key)
			return nil, err
		}
	}

	return accum, nil
}

func checkRequiredArgs(o *RecursiveConcatOptions) {
	if o.Client == nil {
		Fatalf(context.Background(), "s3 client is required")
	}
	if o.Bucket == "" {
		Fatalf(context.Background(), "Bucket is required")
	}
	if o.DstPrefix == "" {
		Fatalf(context.Background(), "DstPrefix is required")
	}
	if o.Region == "" {
		Fatalf(context.Background(), "Region is required")
	}
}

// Copy creates a clone where the APIOptions list is deep copied.
func (o RecursiveConcatOptions) Copy() RecursiveConcatOptions {
	to := o
	return to
}
