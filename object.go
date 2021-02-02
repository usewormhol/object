package object

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/usewormhol/env"
	"github.com/usewormhol/random"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cloudflare/cloudflare-go"
)

type Object struct {
	Key                 string            `json:"key"`
	Name                string            `json:"name"`
	SizeBytes           int64             `json:"size"`
	LastModified        time.Time         `json:"last_modified"`
	ExpirationSeconds   int64             `json:"expiration"`
	PresignedGetUrl     string            `json:"presigned_get_url"`
	PresignedPutUrl     string            `json:"presigned_put_url"`
	PresignedPutHeaders map[string]string `json:"presigned_put_headers"`
}

var (
	S3_REGION                   = env.String("WORMHOL_S3_REGION", "", env.Required)
	S3_ACCESS_KEY_ID            = env.String("WORMHOL_S3_ACCESS_KEY_ID", "", env.Required)
	S3_SECRET_ACCESS_KEY        = env.String("WORMHOL_S3_SECRET_ACCESS_KEY", "", env.Required)
	S3_BUCKET                   = env.String("WORMHOL_S3_BUCKET", "", env.Required)
	S3_ACL                      = env.String("WORMHOL_S3_ACL", s3.ObjectCannedACLPrivate, env.Optional)
	S3_SSE                      = env.String("WORMHOL_S3_SSE", s3.ServerSideEncryptionAes256, env.Optional)
	S3_STORAGE_CLASS            = env.String("WORMHOL_S3_STORAGE_CLASS", s3.ObjectStorageClassOnezoneIa, env.Optional)
	S3_LIST_OBJECTS_MAX_KEYS    = env.Int64("WORMHOL_S3_LIST_OBJECTS_MAX_KEYS", 1000, env.Optional)
	CLOUDFLARE_ZONE             = env.String("WORMHOL_CLOUDFLARE_ZONE", "", env.Optional)
	CLOUDFLARE_HOST             = env.String("WORMHOL_CLOUDFLARE_HOST", "", env.Optional)
	CLOUDFLARE_EMAIL            = env.String("WORMHOL_CLOUDFLARE_EMAIL", "", env.Optional)
	CLOUDFLARE_KEY              = env.String("WORMHOL_CLOUDFLARE_KEY", "", env.Optional)
	CLOUDFLARE_TOKEN            = env.String("WORMHOL_CLOUDFLARE_TOKEN", "", env.Optional)
	CLOUDFLARE_USER_SERVICE_KEY = env.String("WORMHOL_CLOUDFLARE_USER_SERVICE_KEY", "", env.Optional)
	OBJECT_KEY_BASE             = env.String("WORMHOL_OBJECT_KEY_BASE", "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", env.Optional)
	OBJECT_KEY_LENGTH           = env.Int("WORMHOL_OBJECT_KEY_LENGTH", 4, env.Optional)
	OBJECT_KEY_DELAY_MAX        = time.Duration(env.Int("WORMHOL_OBJECT_KEY_DELAY_MAX_SECONDS", 5, env.Optional)) * time.Second
	OBJECT_NAME_LENGTH_MIN      = env.Int("WORMHOL_OBJECT_NAME_LENGTH_MIN", 1, env.Optional)
	OBJECT_NAME_LENGTH_MAX      = env.Int("WORMHOL_OBJECT_NAME_LENGTH_MAX", 255, env.Optional)
	OBJECT_SIZE_MIN             = env.Int64("WORMHOL_OBJECT_SIZE_MIN_BYTES", 0, env.Optional)
	OBJECT_SIZE_MAX             = env.Int64("WORMHOL_OBJECT_SIZE_MAX_BYTES", 5*1000000000, env.Optional)
	OBJECT_TIME_TO_LIVE         = time.Duration(env.Int("WORMHOL_OBJECT_TIME_TO_LIVE_SECONDS", 60*60*24*3-1, env.Optional)) * time.Second

	errObjectKeyInvalid                = errors.New("object key invalid")
	errObjectNameInvalid               = errors.New("object name invalid")
	errObjectSizeInvalid               = errors.New("object size invalid")
	errObjectKeyGenerationTookMaxDelay = errors.New("object key generation took max delay")

	s3Client = s3.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(S3_REGION),
		Credentials: credentials.NewStaticCredentials(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, ""),
	})), nil)

	cloudflareClient *cloudflare.API

	randomStringGenerator = random.NewStringGenerator(OBJECT_KEY_BASE)
)

func Store(name string, size int64) (*Object, error) {
	err := objectValidate(nil, &name, &size)
	if err != nil {
		return nil, err
	}

	key, err := objectGenerateUniqueKey(&OBJECT_KEY_DELAY_MAX)
	if err != nil {
		return nil, err
	}
	contentDisposition := fmt.Sprintf(`attachment; filename="%s"`, name)

	req, _ := s3Client.PutObjectRequest(&s3.PutObjectInput{
		ACL:                  aws.String(S3_ACL),
		Bucket:               aws.String(S3_BUCKET),
		ContentDisposition:   aws.String(contentDisposition),
		ContentLength:        aws.Int64(size),
		Key:                  aws.String(key),
		ServerSideEncryption: aws.String(S3_SSE),
		StorageClass:         aws.String(S3_STORAGE_CLASS),
	})

	url, err := req.Presign(time.Hour * 24)
	if err != nil {
		return nil, err
	}

	return &Object{
		Key:             key,
		PresignedPutUrl: url,
		PresignedPutHeaders: map[string]string{
			"Content-Disposition":          contentDisposition,
			"X-AMZ-Acl":                    S3_ACL,
			"X-AMZ-Server-Side-Encryption": S3_SSE,
			"X-AMZ-Storage-Class":          S3_STORAGE_CLASS,
		},
	}, nil
}

func Retrieve(key string) (*Object, error) {
	err := objectValidate(&key, nil, nil)
	if err != nil {
		return nil, err
	}

	object, err := s3Client.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(S3_BUCKET), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}

	name, err := url.QueryUnescape(strings.Split(*object.ContentDisposition, `"`)[1])
	if err != nil {
		return nil, err
	}

	req, _ := s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(S3_BUCKET),
		Key:    aws.String(key),
	})

	url, err := req.Presign(object.LastModified.Add(OBJECT_TIME_TO_LIVE).Sub(time.Now()))
	if err != nil {
		return nil, err
	}

	return &Object{
		Key:               key,
		Name:              name,
		SizeBytes:         *object.ContentLength,
		ExpirationSeconds: object.LastModified.Add(OBJECT_TIME_TO_LIVE).UTC().Unix(),
		PresignedGetUrl:   url,
	}, nil
}

func List() ([]*Object, error) {
	var objects []*Object

	out, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(S3_BUCKET),
		MaxKeys: aws.Int64(S3_LIST_OBJECTS_MAX_KEYS),
	})
	if err != nil {
		return nil, err
	}

	first := true
	for first || *out.IsTruncated {
		if !first {
			out, err = s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
				Bucket:     aws.String(S3_BUCKET),
				MaxKeys:    aws.Int64(S3_LIST_OBJECTS_MAX_KEYS),
				StartAfter: aws.String(objects[len(objects)-1].Key),
			})
			if err != nil {
				return nil, err
			}
		}

		for _, obj := range out.Contents {
			objects = append(objects, &Object{
				Key:          *obj.Key,
				LastModified: *obj.LastModified,
			})
		}

		first = false
	}

	return objects, nil
}

func (o *Object) Delete() error {
	var err error

	_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(S3_BUCKET),
		Key:    aws.String(o.Key),
	})
	if err != nil {
		return err
	}

	err = o.purgeCache()
	if err != nil {
		return err
	}

	return nil
}

func (o *Object) purgeCache() error {
	if CLOUDFLARE_ZONE != "" {
		var err error

		if CLOUDFLARE_EMAIL != "" && CLOUDFLARE_KEY != "" {
			cloudflareClient, err = cloudflare.New(CLOUDFLARE_KEY, CLOUDFLARE_EMAIL)
		}
		if CLOUDFLARE_TOKEN != "" {
			cloudflareClient, err = cloudflare.NewWithAPIToken(CLOUDFLARE_TOKEN)
		}
		if CLOUDFLARE_USER_SERVICE_KEY != "" {
			cloudflareClient, err = cloudflare.NewWithUserServiceKey(CLOUDFLARE_USER_SERVICE_KEY)
		}
		if err != nil {
			return err
		}

		_, err = cloudflareClient.PurgeCache(CLOUDFLARE_ZONE, cloudflare.PurgeCacheRequest{
			Files: []string{
				fmt.Sprintf("%s/%s", CLOUDFLARE_HOST, o.Key),
				fmt.Sprintf("%s/%s/", CLOUDFLARE_HOST, o.Key),
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func Reap() (int, error) {
	n := 0

	objects, err := List()
	if err != nil {
		return n, err
	}

	now := time.Now()
	for _, obj := range objects {
		if obj.LastModified.Add(OBJECT_TIME_TO_LIVE).Before(now) {
			if err := obj.Delete(); err != nil {
				return n, err
			}
			n++
		}
	}

	return n, nil
}

func objectValidate(key *string, name *string, size *int64) error {
	if key != nil {
		if len(*key) < OBJECT_KEY_LENGTH || OBJECT_KEY_LENGTH*2 < len(*key) {
			return errObjectKeyInvalid
		}
		for _, character := range *key {
			if !strings.Contains(OBJECT_KEY_BASE, string(character)) {
				return errObjectKeyInvalid
			}
		}
	}

	if name != nil {
		*name = url.QueryEscape(*name)
		*name = strings.ReplaceAll(*name, "+", "%20")
		if len(*name) < OBJECT_NAME_LENGTH_MIN || OBJECT_NAME_LENGTH_MAX < len(*name) {
			return errObjectNameInvalid
		}
	}

	if size != nil {
		if *size < OBJECT_SIZE_MIN || OBJECT_SIZE_MAX < *size {
			return errObjectSizeInvalid
		}
	}

	return nil
}

func objectGenerateUniqueKey(maxDelay *time.Duration) (string, error) {
	t_start := time.Now()

	key := randomStringGenerator.Generate(OBJECT_KEY_LENGTH)
	headObjectInput := &s3.HeadObjectInput{Bucket: aws.String(S3_BUCKET), Key: &key}
	_, err := s3Client.HeadObject(headObjectInput)

	for err.(awserr.Error).Code() != "NotFound" {
		if time.Since(t_start) > *maxDelay {
			return "", errObjectKeyGenerationTookMaxDelay
		}
		key = randomStringGenerator.Generate(OBJECT_KEY_LENGTH)
		_, err = s3Client.HeadObject(headObjectInput)
	}

	return key, nil
}
