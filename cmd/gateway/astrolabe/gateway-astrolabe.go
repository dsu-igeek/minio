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
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io"
	"net/http"
	"strings"
	"time"

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

type astrolabeObjects struct {
	minio.GatewayUnsupported
	pem astrolabe.ProtectedEntityManager
	logger *logrus.Logger
}

// NewGatewayLayer returns a new  ObjectLayer.
func (this *Astrolabe) NewGatewayLayer(creds auth.Credentials) (cmd.ObjectLayer, error) {
	dpem := server.NewDirectProtectedEntityManagerFromConfigDir(this.confDir, "")
	return astrolabeObjects {
		pem: dpem,
		logger: logrus.New(),
	}, nil
}

// Returns true if gateway is ready for production.
func (this *Astrolabe)  Production() bool {
	return false
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

func (this astrolabeObjects) listObjectInfo(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (results []minio.ObjectInfo, err error) {
	petm := this.pem.GetProtectedEntityTypeManager(bucket)
	if petm == nil {
		// Set error to something reasonable
		return
	}
	peids, err := petm.GetProtectedEntities(ctx)
	if err != nil {
		return
	}
	for _, curPEID := range peids {
		results, err = this.appendObjectNamesForID(ctx, curPEID, results)
		if err != nil {
			this.logger.Errorf("Error retrieving names for %s:%v", curPEID.String(), err)
			continue;
		}
		pe, err := petm.GetProtectedEntity(ctx, curPEID)
		if err != nil {
			this.logger.Errorf("Error retrieving protected entity for %s:%v", curPEID.String(), err)
			continue
		}
		snapshots, err := pe.ListSnapshots(ctx)
		if err != nil {
			this.logger.Errorf("Error retrieving snapshots for %s:%v", curPEID.String(), err)
			continue
		}
		for _, curSnapshotID := range snapshots {
			snapshotPEID := curPEID.IDWithSnapshot(curSnapshotID)
			results, err = this.appendObjectNamesForID(ctx, snapshotPEID, results)
			if err != nil {
				this.logger.Errorf("Error retrieving names for %s:%v", curPEID.String(), err)
				continue;
			}
		}
	}
	return
}

func (this astrolabeObjects) appendObjectNamesForID(ctx context.Context, curPEID astrolabe.ProtectedEntityID, appendResults []minio.ObjectInfo) (results []minio.ObjectInfo, err error) {
	results = appendResults
	pe, err := this.pem.GetProtectedEntity(ctx, curPEID)
	if err == nil {
		objectInfo, err := getObjectInfo(ctx, pe)
		if err == nil {
			results = append(results, objectInfo)
			results = append(results, minio.ObjectInfo{
				Name: curPEID.GetID() + ".md",
			})
			results = append(results, minio.ObjectInfo{
				Name: curPEID.GetID() + ".zip",
			})
		}
	}
	return
}

func (this astrolabeObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result cmd.ListObjectsInfo, err error) {
	result.Objects, err = this.listObjectInfo(ctx, bucket, prefix, marker, delimiter, maxKeys)
	return
}

func (this astrolabeObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result cmd.ListObjectsV2Info, err error) {
	result.Objects, err = this.listObjectInfo(ctx, bucket, prefix, continuationToken, delimiter, maxKeys)
	return
}

func getObjectInfo(ctx context.Context, pe astrolabe.ProtectedEntity) (objInfo minio.ObjectInfo, err error){
	info, err := pe.GetInfo(ctx)
	if err == nil {
		objInfo = minio.ObjectInfo{
			Bucket:          pe.GetID().GetPeType(),
			Name:            pe.GetID().String(),
			ModTime:         time.Time{},
			Size:            info.GetSize(),
			IsDir:           false,
			ETag:            "",
			ContentType:     "",
			ContentEncoding: "",
			Expires:         time.Time{},
			StorageClass:    "",
			UserDefined:     nil,
			Parts:           nil,
			Writer:          nil,
			Reader:          nil,
			PutObjReader:    nil,
			AccTime:         time.Time{},
		}
	}
	return
}

func (this astrolabeObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *cmd.HTTPRangeSpec, h http.Header, lockType cmd.LockType, opts cmd.ObjectOptions) (reader *cmd.GetObjectReader, err error) {
	pe, err := this.getPEForBucketObject(ctx, bucket, object)
	if err != nil {
		err = minio.ErrorRespToObjectError(err, bucket, object)
		return
	}
	var objInfo minio.ObjectInfo
	objInfo, err = getObjectInfo(ctx, pe)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	var startOffset int64
	startOffset, _, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	dr, err := pe.GetDataReader(ctx)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	if rs, ok := interface{}(dr).(io.Seeker); ok {
		_, err := rs.Seek(startOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	drCloser := func() { dr.Close() }
	return minio.NewGetObjectReaderFromReader(dr, objInfo, opts.CheckCopyPrecondFn, drCloser)
}

func (a astrolabeObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts cmd.ObjectOptions) (err error) {
	panic("implement me")
}

func (this astrolabeObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (objInfo cmd.ObjectInfo, err error) {
	pe, err := this.getPEForBucketObject(ctx, bucket, object)
	if err != nil {
		err = minio.ErrorRespToObjectError(err, bucket, object)
		return
	}
	objInfo, err = getObjectInfo(ctx, pe)
	return
}

func (this astrolabeObjects) getPEForBucketObject(ctx context.Context, bucket string, object string) (pe astrolabe.ProtectedEntity, err error) {
	petm := this.pem.GetProtectedEntityTypeManager(bucket)
	if petm == nil {
		err = minio.BucketNotFound{
			Bucket: bucket,
			Object: object,
		}
		return
	}
	if strings.Contains(object, "/") {
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
		return
	}
	peidStr := object
	if strings.Contains(peidStr, ".") {
		if strings.HasSuffix(peidStr, ".md") {
			peidStr = strings.TrimSuffix(peidStr, ".md")
		}
		if strings.HasSuffix(peidStr, ".zip") {
			peidStr = strings.TrimSuffix(peidStr, ".zip")
		}
	}
	peid, err := astrolabe.NewProtectedEntityIDFromString(peidStr)
	if err != nil {
		err = minio.ErrorRespToObjectError(err, bucket, object)
		return
	}
	pe, err = petm.GetProtectedEntity(ctx, peid)
	return
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
	return false
}

func (a astrolabeObjects) IsListenBucketSupported() bool {
	return false
}

func (a astrolabeObjects) IsEncryptionSupported() bool {
	return false
}

func (a astrolabeObjects) IsCompressionSupported() bool {
	return false
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


