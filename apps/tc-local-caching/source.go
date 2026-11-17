package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type objectSource struct {
	client *s3.Client
	bucket string
}

func newObjectSource() (objectSource, error) {
	endpoint, bucket := os.Getenv("SILO_ENDPOINT"), os.Getenv("SILO_BUCKET")
	user, password := os.Getenv("SILO_ACCESS_KEY"), os.Getenv("SILO_SECRET_KEY")
	if endpoint == "" || bucket == "" || user == "" || password == "" {
		return objectSource{}, errors.New("SILO_ENDPOINT, SILO_BUCKET, SILO_ACCESS_KEY and SILO_SECRET_KEY are required")
	}
	client := s3.NewFromConfig(aws.Config{
		Region:           "us-east-1",
		RetryMaxAttempts: 1,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: user, SecretAccessKey: password}, nil
		}),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	return objectSource{client, bucket}, nil
}

func (s objectSource) OpenObject(ctx context.Context, key string) (io.ReadCloser, error) {
	res, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (s objectSource) seed(ctx context.Context, pinsPath string) error {
	for {
		_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
		var exists *types.BucketAlreadyOwnedByYou
		if err == nil || errors.As(err, &exists) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("prepare Silo bucket: %w", err)
		case <-time.After(400 * time.Millisecond):
		}
	}
	refs := map[string]request{}
	versions, err := fixtures.ReadDir("fixtures")
	if err != nil {
		return err
	}
	for _, v := range versions {
		objects, req, err := fixtureObjects(v.Name())
		if err != nil {
			return err
		}
		// Publish the manifest last so it cannot reference missing bundles.
		for key, data := range objects {
			if key == req.ManifestKey {
				continue
			}
			if err := s.publish(ctx, key, data); err != nil {
				return err
			}
		}
		if err := s.publish(ctx, req.ManifestKey, objects[req.ManifestKey]); err != nil {
			return err
		}
		refs[v.Name()] = req
	}
	if err := os.MkdirAll(filepath.Dir(pinsPath), 0750); err != nil {
		return err
	}
	data, err := json.Marshal(refs)
	if err != nil {
		return err
	}
	return os.WriteFile(pinsPath, data, 0644)
}

func (s objectSource) publish(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(data), IfNoneMatch: aws.String("*"),
		ContentType: aws.String("application/octet-stream"),
	})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
		return fmt.Errorf("publish %s: %w", key, err)
	}
	body, err := s.OpenObject(ctx, key)
	if err != nil {
		return err
	}
	defer body.Close()
	existing, err := io.ReadAll(io.LimitReader(body, int64(len(data))+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("object %s already exists with different bytes", key)
	}
	return nil
}

// A local immutable object directory is useful for deterministic socket tests.
type directorySource string

func (s directorySource) OpenObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(string(s))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.Open(key)
}
func configuredSource() (BundleSource, error) {
	if root := os.Getenv("TC_OBJECT_ROOT"); root != "" {
		return directorySource(root), nil
	}
	return newObjectSource()
}
