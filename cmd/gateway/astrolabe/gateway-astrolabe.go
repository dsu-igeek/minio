package astrolabe

import (
	"context"
	"github.com/minio/cli"
	"github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/lifecycle"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io"
	"net/http"

	minio "github.com/minio/minio/cmd"

	"github.com/vmware-tanzu/astrolabe/pkg/server"
)

type Astrolabe struct {
	confDir string
}

func init() {
	const astrolabeGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Minio requires this to be set but will be ignored
     MINIO_SECRET_KEY: Minio requires this to be set but will be ignored

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to MinIO host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ",".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ",".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_QUOTA: Maximum permitted usage of the cache in percentage (0-100).

EXAMPLES:
  1. Start minio gateway server for Astrolabe backend.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accountID
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}applicationKey
     {{.Prompt}} {{.HelpName}}

  2. Start minio gateway server for B2 backend with edge caching enabled.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accountID
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}applicationKey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXPIRY{{.AssignmentOperator}}40
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}80
     {{.Prompt}} {{.HelpName}}
`
	minio.RegisterGatewayCommand(cli.Command{
		Name:               "astrolabe",
		Usage:              "Astrolabe Data Protection API",
		Action:             astrolabeGatewayMain,
		CustomHelpTemplate: astrolabeGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway b2' command line.
func astrolabeGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "astrolabe", 1)
	}

	//httpClient := &http.Client{Transport: minio.NewCustomHTTPTransport()}

	astrolabeObjects := Astrolabe {
		confDir: ctx.Args().First(),
	}

	minio.StartGateway(ctx, &astrolabeObjects)
}


// Name returns the unique name of the gateway.
func (this *Astrolabe) Name() string {
	return "astrolabe"
}

// NewGatewayLayer returns a new  ObjectLayer.
func (this *Astrolabe) NewGatewayLayer(creds auth.Credentials) (cmd.ObjectLayer, error) {
	dpem := server.NewDirectProtectedEntityManagerFromConfigDir(this.confDir, "")
	return astrolabeObjects {
		pem: dpem,
	}, nil
}

// Returns true if gateway is ready for production.
func (this *Astrolabe)  Production() bool {
	return false
}

type astrolabeObjects struct {
	minio.GatewayUnsupported
	pem astrolabe.ProtectedEntityManager
}


func (this astrolabeObjects) Shutdown(context context.Context) error {
	return nil
}

func (this astrolabeObjects) StorageInfo(context context.Context) (si minio.StorageInfo) {
	si.Backend.Type = minio.BackendGateway
	si.Backend.GatewayOnline = true		// Astrolabe may be talking to a number of different backends, so we won't track
										// those individually
	return si
}

func (this astrolabeObjects) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	panic("implement me")
}

func (this astrolabeObjects) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo cmd.BucketInfo, err error) {
	petm := this.pem.GetProtectedEntityTypeManager(bucket)
	if petm != nil {
		bucketInfo = bucketInfoForEntityManager(petm)
	} else {
		err = minio.BucketNotFound{Bucket: bucket}
	}
	return
}

func (this astrolabeObjects) ListBuckets(ctx context.Context) (buckets []cmd.BucketInfo, err error) {
	for _, petm := range this.pem.ListEntityTypeManagers() {
		buckets = append(buckets, bucketInfoForEntityManager(petm))
	}
	return
}

func bucketInfoForEntityManager(petm astrolabe.ProtectedEntityTypeManager) (bucketInfo cmd.BucketInfo) {
	bucketInfo.Name = petm.GetTypeName()
	return
}

func (a astrolabeObjects) DeleteBucket(ctx context.Context, bucket string) error {
	panic("implement me")
}

func (a astrolabeObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result cmd.ListObjectsInfo, err error) {
	panic("implement me")
}

func (this astrolabeObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result cmd.ListObjectsV2Info, err error) {
	petm := this.pem.GetProtectedEntityTypeManager(bucket)
	peids, err := petm.GetProtectedEntities(ctx)
	if err != nil {

	}
	for _, curPEID := range peids {
		result.Objects = append(result.Objects, minio.ObjectInfo {
			Name: curPEID.GetID(),
		})
		result.Objects = append(result.Objects, minio.ObjectInfo {
			Name: curPEID.GetID() + ".md",
		})
		result.Objects = append(result.Objects, minio.ObjectInfo {
			Name: curPEID.GetID() + ".zip",
		})
	}
	return
}

func (a astrolabeObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *cmd.HTTPRangeSpec, h http.Header, lockType cmd.LockType, opts cmd.ObjectOptions) (reader *cmd.GetObjectReader, err error) {
	panic("implement me")
}

func (a astrolabeObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts cmd.ObjectOptions) (err error) {
	panic("implement me")
}

func (a astrolabeObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (objInfo cmd.ObjectInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) PutObject(ctx context.Context, bucket, object string, data *cmd.PutObjReader, opts cmd.ObjectOptions) (objInfo cmd.ObjectInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo cmd.ObjectInfo, srcOpts, dstOpts cmd.ObjectOptions) (objInfo cmd.ObjectInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	panic("implement me")
}

func (a astrolabeObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	panic("implement me")
}

func (a astrolabeObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result cmd.ListMultipartsInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (uploadID string, err error) {
	panic("implement me")
}

func (a astrolabeObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo cmd.ObjectInfo, srcOpts, dstOpts cmd.ObjectOptions) (info cmd.PartInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *cmd.PutObjReader, opts cmd.ObjectOptions) (info cmd.PartInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts cmd.ObjectOptions) (result cmd.ListPartsInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	panic("implement me")
}

func (a astrolabeObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []cmd.CompletePart, opts cmd.ObjectOptions) (objInfo cmd.ObjectInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	panic("implement me")
}

func (a astrolabeObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	panic("implement me")
}

func (a astrolabeObjects) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	panic("implement me")
}

func (a astrolabeObjects) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool, scanMode madmin.HealScanMode) (madmin.HealResultItem, error) {
	panic("implement me")
}

func (a astrolabeObjects) HealObjects(ctx context.Context, bucket, prefix string, healObjectFn func(string, string) error) error {
	panic("implement me")
}

func (a astrolabeObjects) ListBucketsHeal(ctx context.Context) (buckets []cmd.BucketInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result cmd.ListObjectsInfo, err error) {
	panic("implement me")
}

func (a astrolabeObjects) SetBucketPolicy(context.Context, string, *policy.Policy) error {
	panic("implement me")
}

func (a astrolabeObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketAction,
					policy.GetObjectAction,
					policy.ListAllMyBucketsAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil}

func (a astrolabeObjects) DeleteBucketPolicy(context.Context, string) error {
	panic("implement me")
}

func (a astrolabeObjects) IsNotificationSupported() bool {
	panic("implement me")
}

func (a astrolabeObjects) IsListenBucketSupported() bool {
	panic("implement me")
}

func (a astrolabeObjects) IsEncryptionSupported() bool {
	panic("implement me")
}

func (a astrolabeObjects) IsCompressionSupported() bool {
	panic("implement me")
}

func (a astrolabeObjects) SetBucketLifecycle(context.Context, string, *lifecycle.Lifecycle) error {
	panic("implement me")
}

func (a astrolabeObjects) GetBucketLifecycle(context.Context, string) (*lifecycle.Lifecycle, error) {
	panic("implement me")
}

func (a astrolabeObjects) DeleteBucketLifecycle(context.Context, string) error {
	panic("implement me")
}

func (a astrolabeObjects) GetMetrics(ctx context.Context) (*cmd.Metrics, error) {
	panic("implement me")
}


