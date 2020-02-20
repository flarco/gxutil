package gxutil

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3 is a AWS s3 object
type S3 struct {
	Bucket string
	Region string
}

// WriteStream  write to an S3 bucket (upload)
// Example: Database or CSV stream into S3 file
func (s *S3) WriteStream(key string, reader io.Reader) error {
	s.GetRegion()

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))
	uploader := s3manager.NewUploader(sess)
	uploader.Concurrency = 10

	// Upload the file to S3.
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return nil
}

type fakeWriterAt struct {
	w io.Writer
}

func (fw fakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

// ReadStream read from an S3 bucket (download)
// Example: S3 file stream into Database or CSV
func (s *S3) ReadStream(key string) (*io.PipeReader, error) {
	s.GetRegion()

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		// Write the contents of S3 Object to the file
		_, err := downloader.Download(
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(s.Bucket),
				Key:    aws.String(key),
			})
		Check(err, "Error downloading S3 File -> "+key)
		pipeW.Close()
	}()

	return pipeR, nil
}

// GetRegion determines the region of the bucket
func (s *S3) GetRegion() (region string) {

	if s.Region != "" {
		return s.Region
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:                    credentials.NewEnvCredentials(),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
	}))
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), sess, s.Bucket, "us-east-1")
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			fmt.Fprintf(os.Stderr, "unable to find bucket %s's region not found\n", s.Bucket)
		}
		LogError(Error(err, "Bucket not found for "+s.Bucket))
	}

	s.Region = region
	return region
}

// Delete deletes an s3 object at provided key
func (s *S3) Delete(key string) (err error) {
	s.GetRegion()
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:                    credentials.NewEnvCredentials(),
		Region:                         aws.String(s.Region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	paths, err := s.List(key)
	if err != nil {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, path := range paths {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(path)})
	}

	_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(s.Bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	})

	if err != nil {
		return Error(err, "Unable to delete S3 object: "+key)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	return err
}

// List S3 objects from a key/prefix
func (s *S3) List(key string) (paths []string, err error) {
	s.GetRegion()
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int64(100000),
	}

	result, err := svc.ListObjectsV2(input)
	if err != nil {
		return paths, err
	}

	for _, obj := range result.Contents {
		// paths = append(paths, F(`s3://%s/%s`, s.Bucket, *obj.Key))
		paths = append(paths, *obj.Key)
	}

	sort.Strings(paths)
	return paths, err
}
